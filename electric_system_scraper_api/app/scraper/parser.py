from bs4 import BeautifulSoup
from config import logger  # Correct import path for logger

class Parser:
    """
    Handles parsing of HTML content to extract table data.
    """
    def parse_table(self, html_content):
        """
        Parses an HTML table and extracts its headers and rows.

        Args:
            html_content (str): Raw HTML content.

        Returns:
            tuple: A tuple containing a list of headers and a list of rows.
        """
        logger.info("Parsing HTML content...")
        soup = BeautifulSoup(html_content, "html.parser")
        table = soup.find("table", {"border": "1"})

        if not table:
            logger.warning("No table found in the HTML content.")
            return [], []

        # Extract headers
        headers = [th.get_text(strip=True) for th in table.find_all("th")]

        # Extract rows
        rows = []
        for tr in table.find_all("tr")[1:]:  # Skip the header row
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) == len(headers):
                rows.append(cells)

        logger.info(f"Parsed table successfully. Rows extracted: {len(rows)}")
        return headers, rows