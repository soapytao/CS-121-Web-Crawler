import logging
from datamodel.search.Stao3_datamodel import Stao3Link, OneStao3UnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs, urljoin
from uuid import uuid4

from io import StringIO

MAX_NUM_LINKS = 3000
MAX_OUTLINKS = 0
MAX_OUTLINKS_URL = 0
subdomains = dict()

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(Stao3Link)
@GetterSetter(OneStao3UnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "Stao3"

    def __init__(self, frame):
        self.app_id = "Stao3"
        self.frame = frame


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneStao3UnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = Stao3Link("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneStao3UnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url.encode('utf-8')
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(Stao3Link(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    global MAX_OUTLINKS
    global MAX_OUTLINKS_URL
    global subdomains

    try:
        # Check if link is redirected
        to_parse = rawDataObj.url
        if rawDataObj.is_redirected:
            to_parse = rawDataObj.final_url
        parsed = urlparse(to_parse)
    
        # Check if the page is empty
        if rawDataObj.content == "":
            return outputLinks
 
        # Check if there is an error message
        if rawDataObj.error_message:
            return outputLinks

        doc = html.fromstring(rawDataObj.content)   # Turns content string into html doc
        scraped_urls = set(doc.xpath('//a/@href'))   # Returns a set of the links on root page
        
        for url in scraped_urls:
            #Keep track of # of different URLs processed from this subdomain
            try:
                subdomains[parsed.netloc] += 1
            except KeyError:
                subdomains[parsed.netloc] = 1

            #Turn scraped URLs into absolute URLs
            parsed_scraped = urlparse(url)
            if (parsed_scraped.scheme == ""):
                absolute_url = urljoin(parsed.scheme + "://" + parsed.netloc, url)
            else:
                absolute_url = url
            
            outputLinks.append(absolute_url)
            
    except:
        return outputLinks

    # Check and update most outlinks
    if len(outputLinks) > MAX_OUTLINKS:
        MAX_OUTLINKS = len(outputLinks)
        MAX_OUTLINKS_URL = rawDataObj.url

    # Write analytics to file
    with open('analytics.txt', 'w') as file:
        file.write("URL with the most outlinks: " + str(MAX_OUTLINKS_URL) + "\n")
        file.write("Number of outlinks: " + str(MAX_OUTLINKS) + "\n\n")
        file.write("Subdomains visited and # of URLs processed in each subdomain:\n")
        for sd, num in subdomains.iteritems():
            file.write(str(sd) + " --- " + str(num) + "\n")
        
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        # NOTE: I found a few of my regular expression patterns from this website:
        # https://support.archive-it.org/hc/en-us/articles/208332963-Modify-crawl-scope-with-a-Regular-Expression
        
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*calendar.*|.*mailto:.*"\
            + "|.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*"\
            + "|.*(/misc|/sites|/all|/themes|/modules|/profiles|/css|/field|/node|/theme){3}.*"\
            + "|.*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|php\?|txt"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False

