import re
import requests
import bs4

from _constants import GENERALINFO, BUILDING, TAX, SALES, IMAGES, COMPS, APPEAL, MAP
from _constants import OWNER
from _constants import SPAN
from _constants import SPACE

##########################################################################################
# Scraping Tools
##########################################################################################


def get_county_property_assessment(parcel_id, pages={GENERALINFO: None}, verbose=False):
    """
    Grabs the raw HTML of a property from the county's website.

    Arguments:
        parcel_id: str
        pages: dict[]
            The key of key-value pair must be a case-sensitive string literal of the page to be scraped.
            Defaults to just scraping the initial landing page, General Info.
            As of June 2020, the options are as follows:
            GeneralInfo, Building, Tax, Sales, Images, Comps, Appeal, Map
        verbose: bool
            Toggles printing scraping info to the screen
            Defaults True. Todo: Expand upon verbose and logging. It's currently not a useful toggle as logging is lackluster

    Returns:
        str
            The response.text
        dict[str]
            A dictionary where the key is the scraped page's name and the value is the raw html
    """

    # Validates pages argument:
    for key in pages:
        if key not in [GENERALINFO, BUILDING, TAX, SALES, IMAGES, COMPS, APPEAL, MAP]:
            raise KeyError(
                "Allegheny County's website does not support the given search term"
            )

    COUNTY_REAL_ESTATE_URL = "http://www2.county.allegheny.pa.us/RealEstate/"

    URL_ENDING = ".aspx?"

    search_parameters = {
        "ParcelID": parcel_id,
        "SearchType": 3,
        "SearchParcel": parcel_id,
    }

    if verbose:
        print("Scraping parcel " + parcel_id)
    for page in pages:
        try:
            response = requests.get(
                (COUNTY_REAL_ESTATE_URL + page + URL_ENDING),
                params=search_parameters,
                timeout=5,
            )
            prop_assessment = response.text
        except requests.exceptions.Timeout:
            # TODO: log_error().
            raise requests.exceptions.Timeout
    if len(pages) == 1:
        return prop_assessment
    return pages


##########################################################################################
# Parsing Tools
##########################################################################################


def _soupify_html(raw_html):
    return bs4.BeautifulSoup(raw_html, "html.parser")


def _extract_elementlist_from_soup(soup, element_id, element=SPAN, remove_tags=True):
    """
    Arguments:
        soup: bs4.element.NavigableString
            Note: bs4.element.Tag are accepted and filtered out
        element_id: str

    Returns:
        list[str,]
    # Todo: Refactor into function it is called from?
    """
    # Although most keys work fine, addresses in particular return something like
    # ['1267\xa0BRINTON  RD', <br/>, 'PITTSBURGH,\xa0PA\xa015221'] which needs to be escaped
    content = soup.find(element, id=element_id).contents
    if remove_tags != True:
        return content

    cleaned_content = []
    for tag in content:
        if isinstance(
            tag, bs4.element.Tag
        ):  # Example: <br/> when evaluating the address
            continue
        cleaned_content.append(tag)

    if len(cleaned_content) != 0:
        # Todo: Log error here
        return cleaned_content
    return cleaned_content


def _strip_whitespace(text):
    return re.sub(" {2,}", " ", text).strip()


##########################################################################################
# Combination Functions (OwnerName)
##########################################################################################


class OwnerName:
    __slots__ = ["raw", "clean", "first", "last", "multientity"]

    def __init__(self, parid=None):
        self.multientity = None

    @classmethod
    def get_Owner_given_parid(cls, parid):
        """ Factory method for creating OwnerNames from parcel ids. """
        o = OwnerName()
        html = get_county_property_assessment(parid, pages={GENERALINFO: None})
        soup = _soupify_html(html)
        o.raw = o._parse_owners_from_soup(parid, soup)
        o.clean = (
            o._clean_raw_name()
        )  # Method side effect: May change flag o.multientity
        o.first, o.last = o.split_name()
        return o

    def _parse_owners_from_soup(self, parid, soup):
        return _extract_elementlist_from_soup(
            soup, element_id=OWNER, element=SPAN, remove_tags=True
        )

    def _clean_raw_name(self) -> str:
        if len(self.raw) > 1:
            self.multientity = True
            cleaned_names = []
            for name in self.raw:
                cleaned_names.append(_strip_whitespace(name))
            return ", ".join(cleaned_names)
        return _strip_whitespace(self.raw[0])

    def split_name(self) -> (str, str):
        """
        An attempt to break a name into its surname and firstname.

        This is obviously very flawed methodology, especially given owners aren't necessarily people (be it a coorporation
        or a trust) and should not be relied on. Use the full name whenever you can.
        ww.w3.org/International/questions/qa-personal-names.en
        """
        exp = re.compile(r"(\w+|[&])\s+(\w+|[&])\s*(\w*|[&]).*")
        try:
            namegroups = re.search(exp, self.clean)
            if self.multientity:
                return ("", "")
        except TypeError:  # When type is list
            namegroups = SPACE.join(self.clean)
        try:
            last = str(namegroups.group(1))
            if len(namegroups.groups()) == 2:
                first = str(namegroups.group(2)).title()
            elif len(namegroups.groups()) == 3:
                first = (
                    str(namegroups.group(2)).title()
                    + " "
                    + str(namegroups.group(3)).title()
                )
        except AttributeError:
            # LOG ERROR
            first, last = ("", "")
        return (first, last)
