#!/usr/bin/env python3
"""
Script pour analyser les fusions/acquisitions d'entreprises blockchain/crypto
en vérifiant les redirections de sites web et le contenu des pages.
"""

import csv
import requests
import time
import re
import json
from urllib.parse import urlparse, urljoin
from dataclasses import dataclass
from typing import List, Dict, Optional, Set
import logging
from pathlib import Path

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('merger_checker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class Company:
    name: str
    website: str
    original_url: str
    cb_rank: str
    headquarters: str
    description: str
    
@dataclass
class MergerResult:
    company_name: str
    original_website: str
    final_url: str
    redirected: bool
    domain_changed: bool
    merger_indicators: List[str]
    status: str  # "CLOSED", "ACQUIRED_AND_RUNNING", "UNCLEAR"
    confidence: float
    notes: str
    announcement_link: str = ""
    acquirer_name: str = ""

class MergerChecker:
    def __init__(self):
        self.session = requests.Session()
        # User agent plus récent et cohérent
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Paramètres optimisés pour la performance
        self.timeout = 12
        self.max_redirects = 10
        self.retry_count = 2
        self.delay_between_requests = 1.5  # Délai standard entre requêtes
        
        # Configuration Google Custom Search API
        self.google_api_key = ""
        # Custom Search Engine configuré pour recherche sur tout le web
        self.google_cx = ""  # Engine ID fourni par l'utilisateur
        
        # Paramètres pour Google Search avec rate limiting intelligent
        self.google_delay_base = 1  # Délai réduit avec API (1 seconde)
        self.google_delay_current = 1  # Délai actuel
        self.google_delay_max = 5  # Délai maximum réduit (5 secondes)
        self.google_429_count = 0  # Compteur de 429 consécutifs
        self.use_api = True  # Utiliser l'API par défaut
        
        # Mots-clés stricts pour détecter acquisitions
        self.acquisition_keywords = [
            'acquired by', 'racheté par', 'acquisition par',
            'merged with', 'fusionné avec', 'merger with',
            'now part of', 'subsidiary of', 'division of',
            'purchased by', 'bought by', 'takeover by'
        ]
        
        # Mots-clés pour fermeture
        self.closure_keywords = [
            'ceased operations', 'shut down', 'closed permanently', 'discontinued',
            'no longer operating', 'out of business', 'suspended operations',
            'fermeture définitive', 'cessation d\'activité', 'arrêt des opérations'
        ]
        
        # Domaines de parking/redirection
        self.parking_domains = {
            'godaddy.com', 'namecheap.com', 'squarespace.com', 
            'wix.com', 'wordpress.com', 'github.io',
            'parked-content.godaddy.com', 'afternic.com',
            'sedoparking.com', 'parkingcrew.net'
        }
        
        # Domaines de sources fiables pour les annonces
        self.reliable_news_domains = {
            'businesswire.com', 'prnewswire.com', 'techcrunch.com', 
            'reuters.com', 'bloomberg.com', 'coindesk.com', 'cointelegraph.com',
            'venturebeat.com', 'crunchbase.com', 'finance.yahoo.com',
            'marketwatch.com', 'forbes.com', 'wsj.com', 'ft.com'
        }
        
        # Set pour éviter les doublons
        self.processed_domains = set()

    def configure_custom_search_engine(self, cx_id: str):
        """Configure le Custom Search Engine ID."""
        self.google_cx = cx_id
        self.use_api = True
        logger.info(f"Custom Search Engine configuré: {cx_id}")
        print(f"✅ Custom Search Engine configuré: {cx_id}")
        print("🚀 Prêt pour les recherches Google API !")

    def load_all_companies_deduplicated(self, file_paths: List[str]) -> List[Company]:
        """Charge toutes les entreprises en évitant les doublons par partie comparable."""
        all_companies = []
        seen_parts = set()
        
        for file_path in file_paths:
            if not Path(file_path).exists():
                logger.warning(f"Fichier non trouvé: {file_path}")
                continue
                
            logger.info(f"Chargement de {file_path}")
            companies = self.load_companies_from_csv(file_path)
            
            for company in companies:
                comparable_part = self.get_comparable_part_for_dedup(company.website)
                if comparable_part and comparable_part not in seen_parts:
                    seen_parts.add(comparable_part)
                    all_companies.append(company)
                else:
                    logger.debug(f"Doublon ignoré: {company.name} ({comparable_part})")
        
        return all_companies

    def load_companies_from_csv(self, file_path: str) -> List[Company]:
        """Charge les entreprises depuis un fichier CSV."""
        companies = []
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if row.get('Website') and row.get('Organization Name'):
                        company = Company(
                            name=row['Organization Name'],
                            website=self.clean_url(row['Website']),  # Nettoyer dès le chargement
                            original_url=row.get('Organization Name URL', ''),
                            cb_rank=row.get('CB Rank (Company)', ''),
                            headquarters=row.get('Headquarters Location', ''),
                            description=row.get('Description', '')
                        )
                        companies.append(company)
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {file_path}: {e}")
        
        return companies

    def normalize_domain(self, url: str) -> str:
        """Normalise un domaine pour la comparaison (retire www pour parking domains)."""
        try:
            if not url:
                return ""
            
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Pour les domaines de parking, on retire www pour la comparaison
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return ""

    def normalize_domain_for_comparison(self, url: str) -> str:
        """Normalise un domaine pour la comparaison en gardant www mais en retirant les slashes finaux."""
        try:
            if not url:
                return ""
            
            # Nettoyer l'URL d'abord
            parsed = urlparse(url)
            domain_and_path = parsed.netloc.lower()
            
            # Ajouter le path si présent (sans trailing slash)
            if parsed.path and parsed.path != '/':
                domain_and_path += parsed.path.rstrip('/')
            
            return domain_and_path
        except:
            return ""

    def get_comparable_part(self, url: str) -> str:
        """Extrait la partie comparable d'une URL (ce qui vient après https:// ou https://www.)"""
        try:
            if not url:
                return ""
            
            url = url.strip().rstrip('/')  # Retirer trailing slash
            
            # Retirer le protocole
            if url.startswith('https://'):
                comparable = url[8:]  # Retirer 'https://'
            elif url.startswith('http://'):
                comparable = url[7:]   # Retirer 'http://'
            else:
                comparable = url
            
            return comparable.lower()
        except:
            return ""

    def is_significant_redirect(self, original_url: str, final_url: str) -> bool:
        """Détermine si une redirection est significative en comparant les parties après le protocole."""
        try:
            original_part = self.get_comparable_part(original_url)
            final_part = self.get_comparable_part(final_url)
            
            # Si les parties comparables sont identiques, ce n'est pas significatif
            if original_part == final_part:
                return False
            
            # Extraire le domaine de base (sans le path)
            def get_base_domain(comparable_part):
                # Retirer www si présent
                if comparable_part.startswith('www.'):
                    comparable_part = comparable_part[4:]
                # Prendre seulement la partie avant le premier /
                return comparable_part.split('/')[0]
            
            original_domain = get_base_domain(original_part)
            final_domain = get_base_domain(final_part)
            
            # Si les domaines de base sont différents, c'est significatif
            if original_domain != final_domain:
                return True
            
            # Si même domaine de base, vérifier si c'est juste un ajout de path simple
            # Cas comme example.com → example.com/en ne sont pas significatifs
            original_path = original_part.split('/', 1)[1] if '/' in original_part else ""
            final_path = final_part.split('/', 1)[1] if '/' in final_part else ""
            
            # Si l'original n'avait pas de path et le final en a un court, pas significatif
            if not original_path and final_path and len(final_path.split('/')) <= 2:
                return False
            
            # Sinon, considérer comme non significatif (même domaine)
            return False
        except:
            return False

    def get_comparable_part_for_dedup(self, url: str) -> str:
        """Version pour déduplication qui traite www et non-www comme identiques."""
        try:
            comparable = self.get_comparable_part(url)
            # Retirer www pour déduplication
            if comparable.startswith('www.'):
                comparable = comparable[4:]
            return comparable
        except:
            return ""

    def clean_url(self, url: str) -> str:
        """Nettoie une URL sans retirer les www."""
        if not url:
            return ""
        
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        return url

    def make_simple_request(self, url: str) -> Optional[requests.Response]:
        """Fait une requête simple avec retry limité."""
        for attempt in range(self.retry_count):
            try:
                response = self.session.get(
                    url, 
                    timeout=self.timeout, 
                    allow_redirects=True
                )
                return response
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                if attempt < self.retry_count - 1:
                    time.sleep(1)
                continue
            except Exception as e:
                logger.warning(f"Erreur pour {url}: {e}")
                break
        
        return None

    def enhanced_google_search_acquisition(self, website_url: str, company_name: str) -> Optional[str]:
        """Recherche Google améliorée avec opérateurs et filtrage intelligent."""
        try:
            # Extraire le domaine pour créer une requête plus précise
            domain = self.extract_domain_from_url(website_url)
            
            # Requête optimisée avec opérateurs Google
            # 1. Guillemets pour l'URL exacte
            # 2. AND pour forcer la présence de mots-clés d'acquisition
            # 3. OR pour plusieurs variantes d'acquisition
            query = f'"{domain}" AND ("acquired" OR "acquisition" OR "merger" OR "bought")'
            logger.info(f"  Recherche Google optimisée: {query}")
            
            # ESSAI AVEC L'API D'ABORD
            if self.use_api and self.google_api_key:
                result = self._search_with_api_filtered(query, domain, company_name)
                if result:
                    return result
            
            # FALLBACK HTML
            logger.info(f"  Fallback recherche HTML...")
            result = self._search_with_html_filtered(query, domain, company_name)
            if result:
                return result
            
        except Exception as e:
            logger.warning(f"Erreur recherche Google pour {company_name}: {e}")
        
        return None

    def extract_domain_from_url(self, url: str) -> str:
        """Extrait le domaine principal d'une URL pour la recherche."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Retirer www pour avoir le domaine principal
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return url.replace('https://', '').replace('http://', '').split('/')[0]

    def _search_with_api_filtered(self, query: str, domain: str, company_name: str) -> Optional[str]:
        """Recherche API avec filtrage intelligent."""
        try:
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'q': query,
                'key': self.google_api_key,
                'num': 10,
                'safe': 'active'
            }
            
            # Ajouter cx seulement s'il est défini
            if self.google_cx:
                params['cx'] = self.google_cx
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'items' in data and data['items']:
                    # Filtrer et scorer les résultats
                    scored_results = []
                    
                    for item in data['items']:
                        link = item.get('link', '')
                        title = item.get('title', '')
                        snippet = item.get('snippet', '')
                        
                        # Filtrer les liens Google internes
                        if any(domain_filter in link.lower() for domain_filter in 
                               ['google.com', 'youtube.com', 'maps.google']):
                            continue
                        
                        # Calculer le score de pertinence
                        score = self._calculate_relevance_score(link, title, snippet, domain, company_name)
                        
                        if score > 0:  # Seuil minimum de pertinence
                            scored_results.append((score, link, title))
                    
                    # Trier par score et prendre le meilleur
                    if scored_results:
                        scored_results.sort(reverse=True, key=lambda x: x[0])
                        best_score, best_link, best_title = scored_results[0]
                        logger.info(f"  API - Meilleur résultat (score: {best_score:.2f}): {best_link}")
                        logger.info(f"  Titre: {best_title[:100]}...")
                        return best_link
                
                else:
                    logger.info(f"  API: Aucun résultat")
            
            elif response.status_code == 400:
                logger.warning(f"  API: Besoin d'un Custom Search Engine - passage en HTML")
                self.use_api = False
            
            elif response.status_code == 403:
                logger.warning(f"  API: Quota dépassé - passage en HTML")
                self.use_api = False
            
            else:
                logger.warning(f"  API: Erreur {response.status_code}")
                
        except Exception as e:
            logger.warning(f"  Erreur API: {e}")
            
        return None

    def _calculate_relevance_score(self, link: str, title: str, snippet: str, domain: str, company_name: str) -> float:
        """Calcule un score de pertinence pour un résultat de recherche - FOCUS SUR LE TITRE."""
        score = 0.0
        title_lower = title.lower()
        snippet_lower = snippet.lower()
        link_lower = link.lower()
        
        # 1. TITRE : Phrases d'acquisition explicites (score très élevé)
        acquisition_title_patterns = [
            'acquired by', 'acquires', 'acquisition of', 'purchased by', 'buys', 
            'merger with', 'merges with', 'takeover', 'announces acquisition',
            'completes acquisition', 'acquisition deal', 'acquisition announcement'
        ]
        for pattern in acquisition_title_patterns:
            if pattern in title_lower:
                score += 10.0  # Score très élevé pour titre explicite
        
        # 2. TITRE : Mots-clés d'acquisition dans le titre (score élevé)
        if any(keyword in title_lower for keyword in ['acquired', 'acquisition', 'merger', 'bought']):
            score += 5.0
        
        # 3. TITRE : Nom de l'entreprise dans le titre (obligatoire pour pertinence)
        company_in_title = (company_name.lower() in title_lower or domain.lower() in title_lower)
        if not company_in_title:
            return 0.0  # Si l'entreprise n'est pas dans le titre, score = 0
        else:
            score += 3.0
        
        # 4. Sources d'actualités fiables (bonus important)
        news_domains = [
            'businesswire.com', 'prnewswire.com', 'techcrunch.com', 'reuters.com', 
            'bloomberg.com', 'coindesk.com', 'cointelegraph.com', 'venturebeat.com',
            'marketwatch.com', 'forbes.com', 'wsj.com', 'ft.com', 'cnbc.com'
        ]
        for news_domain in news_domains:
            if news_domain in link_lower:
                score += 7.0  # Bonus élevé pour sources fiables
                break
        
        # 5. PÉNALITÉS LOURDES pour pages génériques non pertinentes
        generic_penalties = [
            ('crunchbase.com', -5.0),       # Profils génériques
            ('wikipedia.', -5.0),           # Pages Wikipedia
            ('linkedin.com', -5.0),         # Profils LinkedIn
            ('reddit.com', -3.0),           # Discussions Reddit
            ('twitter.com', -3.0),          # Tweets
            ('x.com', -3.0),                # Tweets
            ('youtube.com', -3.0),          # Vidéos
            ('podcast', -3.0),              # Podcasts
            ('job', -2.0),                  # Offres d'emploi
            ('career', -2.0),               # Carrières
            ('hiring', -2.0),               # Recrutement
        ]
        
        text_to_check = f"{title_lower} {link_lower}"
        for term, penalty in generic_penalties:
            if term in text_to_check:
                score += penalty
        
        # 6. BONUS pour termes spécifiques d'annonce dans le titre
        announcement_terms = [
            'announces', 'announcement', 'press release', 'news release',
            'official', 'statement', 'confirms', 'completes deal'
        ]
        for term in announcement_terms:
            if term in title_lower:
                score += 2.0
        
        # 7. BONUS pour montants financiers (indique une vraie transaction)
        financial_terms = ['million', 'billion', '$', 'funding', 'valuation', 'deal worth']
        for term in financial_terms:
            if term in (title_lower + ' ' + snippet_lower):
                score += 3.0
                break
        
        # 8. PÉNALITÉS pour titres génériques
        if any(generic in title_lower for generic in [
            'profile', 'overview', 'about', 'company information', 'crunchbase',
            'find podcasters', 'matchmaker', 'directory'
        ]):
            score -= 8.0
        
        return max(0.0, score)  # Score minimum de 0

    def _search_with_html_filtered(self, query: str, domain: str, company_name: str) -> Optional[str]:
        """Recherche HTML avec filtrage intelligent."""
        try:
            google_url = f"https://www.google.com/search"
            params = {
                'q': query,
                'num': 10,
                'hl': 'en',
                'gl': 'us'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            
            response = requests.get(google_url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 429:
                logger.warning(f"  HTML: Rate limited")
                return None
            
            if response.status_code == 200:
                text = response.text
                logger.info(f"  HTML: Page récupérée")
                
                # Patterns pour extraire les liens avec titres
                link_title_patterns = [
                    r'<a[^>]*href="(https?://[^"]+)"[^>]*><h3[^>]*>([^<]+)</h3>',
                    r'<a[^>]*jsname="UWckNb"[^>]*href="(https?://[^"]+)"[^>]*>.*?<h3[^>]*>([^<]+)</h3>',
                ]
                
                # Patterns pour extraire juste les liens
                link_patterns = [
                    r'<a[^>]*href="(https?://[^"]+)"[^>]*><h3',
                    r'<a[^>]*jsname="UWckNb"[^>]*href="(https?://[^"]+)"[^>]*>',
                ]
                
                scored_results = []
                
                # Essayer d'extraire liens avec titres d'abord
                for pattern in link_title_patterns:
                    matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
                    for match in matches:
                        clean_link = match[0].split('&sa=')[0].split('&ved=')[0]
                        title = match[1] if len(match) > 1 else ""
                        
                        # Filtrer les liens Google internes
                        if any(domain_filter in clean_link.lower() for domain_filter in 
                              ['google.com', 'youtube.com', 'maps.google', 'webcache.googleusercontent']):
                            continue
                        
                        # Calculer le score
                        score = self._calculate_relevance_score(clean_link, title, "", domain, company_name)
                        if score > 0:
                            scored_results.append((score, clean_link, title))
                
                # Si pas de résultats avec titres, essayer sans
                if not scored_results:
                    for pattern in link_patterns:
                        matches = re.findall(pattern, text, re.IGNORECASE)
                        for match in matches:
                            clean_link = match.split('&sa=')[0].split('&ved=')[0]
                            
                            # Filtrer les liens Google internes
                            if any(domain_filter in clean_link.lower() for domain_filter in 
                                  ['google.com', 'youtube.com', 'maps.google', 'webcache.googleusercontent']):
                                continue
                            
                            # Score basé seulement sur le lien
                            score = self._calculate_relevance_score(clean_link, "", "", domain, company_name)
                            if score > 0:
                                scored_results.append((score, clean_link, ""))
                
                # Trier et prendre le meilleur
                if scored_results:
                    scored_results.sort(reverse=True, key=lambda x: x[0])
                    best_score, best_link, best_title = scored_results[0]
                    logger.info(f"  HTML - Meilleur résultat (score: {best_score:.2f}): {best_link}")
                    if best_title:
                        logger.info(f"  Titre: {best_title[:100]}...")
                    return best_link
                else:
                    logger.warning(f"  HTML: Aucun lien pertinent trouvé")
            
            else:
                logger.warning(f"  HTML: Code {response.status_code}")
                
        except Exception as e:
            logger.warning(f"  Erreur HTML: {e}")
        
        return None

    def find_announcement_links(self, content: str, base_url: str) -> List[str]:
        """Recherche des liens d'annonce simples sur la page."""
        announcement_links = []
        
        # Patterns simples pour détecter les liens d'annonce
        patterns = [
            r'href="([^"]*(?:announcement|press|news|blog)[^"]*)"',
            r'href="([^"]*)"[^>]*>[^<]*(?:acquisition|merger|acquired)[^<]*</a>',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches[:3]:  # Limiter à 3 liens max
                if match.startswith('http'):
                    announcement_links.append(match)
                elif match.startswith('/'):
                    announcement_links.append(urljoin(base_url, match))
        
        return announcement_links

    def check_website_status(self, company: Company) -> MergerResult:
        """Vérifie le statut d'un site web avec la nouvelle logique simplifiée."""
        logger.info(f"Vérification de {company.name}: {company.website}")
        
        result = MergerResult(
            company_name=company.name,
            original_website=company.website,
            final_url="",
            redirected=False,
            domain_changed=False,
            merger_indicators=[],
            status="UNCLEAR",
            confidence=0.0,
            notes="",
            announcement_link="",
            acquirer_name=""
        )
        
        if not company.website:
            result.notes = "URL invalide"
            result.status = "UNCLEAR"
            return result
        
        # Faire la requête
        response = self.make_simple_request(company.website)
        
        # Si site inaccessible → CLOSED (mais continuer pour recherche Google)
        if response is None:
            result.status = "CLOSED"
            result.confidence = 0.8
            result.notes = "Site inaccessible"
            # Ne pas retourner ici, continuer pour la recherche Google
        else:
            # Analyser la réponse
            result.final_url = response.url
            
            # Vérifier les redirections
            if len(response.history) > 0:
                result.redirected = True
                
            # Vérifier le changement de domaine avec la nouvelle logique
            if self.is_significant_redirect(company.website, response.url):
                result.domain_changed = True
                original_part = self.get_comparable_part(company.website)
                final_part = self.get_comparable_part(response.url)
                result.merger_indicators.append(f"Redirection significative: {original_part} → {final_part}")
                
                # Redirection vers domaine différent → CLOSED
                result.status = "CLOSED"
                result.confidence = 0.7
                result.notes = "Redirection vers domaine différent"
            
            # Si redirection vers domaine de parking → CLOSED
            if self.normalize_domain(response.url) in self.parking_domains:
                result.status = "CLOSED"
                result.confidence = 0.9
                result.notes = "Redirigé vers domaine de parking"
            
            # Si erreur 404 → CLOSED
            elif response.status_code == 404:
                result.status = "CLOSED"
                result.confidence = 0.8
                result.notes = "Erreur 404"
            
            # Si pas de redirection significative → analyser le contenu
            if response.status_code == 200:
                self._analyze_content_for_acquisition(response.text, result, response.url)
            
            # Déterminer le statut final
            self._determine_final_status_revised(result)
        
        # Déterminer le statut final (pour tous les cas)
        if result.status != "CLOSED":  # Si pas déjà défini comme CLOSED
            self._determine_final_status_revised(result)
        
        # RECHERCHE GOOGLE SEULEMENT POUR LES SITES FERMÉS (CLOSED)
        if result.status == "CLOSED":
            logger.info(f"  Site fermé → Recherche Google pour {company.name}...")
            google_result = self.enhanced_google_search_acquisition(company.website, company.name)
            if google_result:
                result.announcement_link = google_result
                result.merger_indicators.append("Lien acquisition trouvé via Google")
                # Augmenter la confiance si on trouve un lien d'acquisition
                if any(keyword in result.announcement_link.lower() for keyword in 
                       ['acquired', 'acquisition', 'merger', 'bought']):
                    result.confidence = min(0.9, result.confidence + 0.2)
        
        # Délai entre requêtes
        time.sleep(self.delay_between_requests)
        
        return result

    def _analyze_content_for_acquisition(self, content: str, result: MergerResult, base_url: str):
        """Analyse le contenu pour détecter les mots-clés d'acquisition."""
        content_lower = content.lower()
        
        # Rechercher les mots-clés d'acquisition
        for keyword in self.acquisition_keywords:
            if keyword in content_lower:
                result.merger_indicators.append(f"Acquisition détectée: '{keyword}'")
                
                # Chercher le nom de l'acquéreur
                pattern = rf'{keyword}\s+([a-zA-Z][a-zA-Z0-9\s&.-]{{2,30}})'
                match = re.search(pattern, content_lower)
                if match:
                    acquirer = match.group(1).strip()
                    result.acquirer_name = acquirer
                    result.merger_indicators.append(f"Acquéreur: {acquirer}")
        
        # Rechercher des liens d'annonce sur la page
        announcement_links = self.find_announcement_links(content, base_url)
        if announcement_links and not result.announcement_link:
            result.announcement_link = announcement_links[0]
            result.merger_indicators.append("Lien d'annonce trouvé sur la page")

    def _determine_final_status_revised(self, result: MergerResult):
        """Détermine le statut final avec la logique révisée."""
        
        # Si déjà défini comme CLOSED, garder ce statut
        if result.status == "CLOSED":
            return
        
        # Analyser les indicateurs d'acquisition
        acquisition_found = any('acquisition détectée' in ind.lower() for ind in result.merger_indicators)
        
        # Logique simple : si pas CLOSED, alors ACQUIRED_AND_RUNNING
        result.status = "ACQUIRED_AND_RUNNING"
        
        if acquisition_found:
            result.confidence = 0.8 if result.acquirer_name else 0.7
            result.notes = "Site accessible avec mots-clés d'acquisition"
        else:
            result.confidence = 0.6
            result.notes = "Site accessible sans indication explicite d'acquisition"

    def save_results(self, results: List[MergerResult], output_file: str):
        """Sauvegarde les résultats dans un fichier CSV."""
        fieldnames = [
            'company_name', 'original_website', 'final_url', 'redirected',
            'domain_changed', 'merger_indicators', 'status', 'confidence', 
            'notes', 'acquirer_name', 'announcement_link'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                writer.writerow({
                    'company_name': result.company_name,
                    'original_website': result.original_website,
                    'final_url': result.final_url,
                    'redirected': result.redirected,
                    'domain_changed': result.domain_changed,
                    'merger_indicators': ' | '.join(result.merger_indicators),
                    'status': result.status,
                    'confidence': result.confidence,
                    'notes': result.notes,
                    'acquirer_name': result.acquirer_name,
                    'announcement_link': result.announcement_link
                })

    def generate_summary(self, results: List[MergerResult]) -> Dict:
        """Génère un résumé des résultats avec les nouveaux statuts."""
        summary = {
            'total_companies': len(results),
            'acquired_and_running': 0,
            'closed': 0,
            'unclear': 0,
            'with_announcement_links': 0,
            'with_acquirer_identified': 0,
            'from_reliable_sources': 0
        }
        
        for result in results:
            if result.status == "ACQUIRED_AND_RUNNING":
                summary['acquired_and_running'] += 1
                if result.announcement_link:
                    summary['with_announcement_links'] += 1
                    # Vérifier si c'est une source fiable
                    if any(domain in result.announcement_link for domain in self.reliable_news_domains):
                        summary['from_reliable_sources'] += 1
                if result.acquirer_name:
                    summary['with_acquirer_identified'] += 1
            elif result.status == "CLOSED":
                summary['closed'] += 1
            else:
                summary['unclear'] += 1
        
        return summary

def main():
    """Fonction principale."""
    csv_files = [
        'data_sources/cluster-0-liste-full-17-05-2025.csv',
        'data_sources/cluster-1-full-17-05-2025.csv',
        'data_sources/cluster-2-full-17-05-2025.csv',
        'data_sources/cluster-4-exits-14-05-2025.csv'
    ]
    
    checker = MergerChecker()
    
    print("🚀 ANALYSE OPTIMISÉE AVEC DÉDUPLICATION ET RECHERCHE GOOGLE")
    print("=" * 70)
    
    # Charger toutes les entreprises en évitant les doublons
    companies = checker.load_all_companies_deduplicated(csv_files)
    
    all_results = []
    
    # Vérification de chaque entreprise unique
    for i, company in enumerate(companies):
        logger.info(f"Progression: {i+1}/{len(companies)}")
        result = checker.check_website_status(company)
        all_results.append(result)
        
        # Sauvegarde intermédiaire tous les 20 résultats
        if (i + 1) % 20 == 0:
            checker.save_results(all_results, 'merger_results_temp.csv')
            logger.info(f"Sauvegarde temporaire effectuée ({len(all_results)} résultats)")
    
    # Sauvegarde finale
    checker.save_results(all_results, 'merger_analysis_results.csv')
    
    # Génération du résumé
    summary = checker.generate_summary(all_results)
    
    # Affichage du résumé
    print("\n" + "="*70)
    print("RÉSUMÉ DE L'ANALYSE")
    print("="*70)
    print(f"Total entreprises analysées: {summary['total_companies']}")
    print(f"Acquises et en cours: {summary['acquired_and_running']} ({summary['acquired_and_running']/summary['total_companies']*100:.1f}%)")
    print(f"Fermées: {summary['closed']} ({summary['closed']/summary['total_companies']*100:.1f}%)")
    print(f"Non déterminées: {summary['unclear']} ({summary['unclear']/summary['total_companies']*100:.1f}%)")
    print(f"Total avec liens d'annonce: {summary['with_announcement_links']}")
    print(f"  - Avec acquéreur identifié: {summary['with_acquirer_identified']}")
    print(f"  - De sources fiables: {summary['from_reliable_sources']}")
    
    # Sauvegarde du résumé
    with open('merger_analysis_summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"\nRésultats sauvegardés dans: merger_analysis_results.csv")
    print(f"Résumé sauvegardé dans: merger_analysis_summary.json")
    print(f"Logs disponibles dans: merger_checker.log")

if __name__ == "__main__":
    main() 