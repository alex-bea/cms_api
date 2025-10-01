#!/usr/bin/env python3
"""
Synthetic HTML Generator for OPPS Scraper Tests
===============================================

Generates controlled HTML pages with various link patterns for testing.
Follows QTS v1.1 requirements for test infrastructure.

Author: CMS Pricing Platform Team
Version: 1.0.0
QTS Compliance: v1.1
"""

from typing import List, Dict, Any
from pathlib import Path
import random


class OPPSSyntheticHTMLGenerator:
    """Generates synthetic HTML pages for OPPS scraper testing."""
    
    def __init__(self, seed: int = 42):
        """Initialize generator with seed for reproducible tests."""
        random.seed(seed)
        self.seed = seed
    
    def generate_quarterly_addenda_page(self, 
                                      num_links: int = 10,
                                      include_broken: bool = True,
                                      include_edge_cases: bool = True) -> str:
        """Generate a synthetic quarterly addenda page."""
        links = []
        
        # Generate valid quarterly links
        quarters = ["January", "April", "July", "October"]
        years = [2024, 2025]
        addenda_types = ["A", "B"]
        
        for i in range(num_links // 2):
            quarter = random.choice(quarters)
            year = random.choice(years)
            addendum_type = random.choice(addenda_types)
            
            link_text = f"{quarter} {year} Addendum {addendum_type}"
            href = f"/{quarter.lower()}-{year}-addendum-{addendum_type.lower()}.csv"
            links.append((href, link_text))
        
        # Add some ZIP files
        for i in range(num_links // 4):
            quarter = random.choice(quarters)
            year = random.choice(years)
            
            link_text = f"{quarter} {year} Addendum"
            href = f"/{quarter.lower()}-{year}-addendum.zip"
            links.append((href, link_text))
        
        # Add edge cases if requested
        if include_edge_cases:
            edge_cases = [
                ("/january-2025-addendum-a%20file.csv", "January 2025 Addendum A File"),
                ("/april-2025-addendum-b+file.xlsx", "April 2025 Addendum B File"),
                ("/july-2025-addendum_underscore.zip", "July 2025 Addendum Underscore"),
                ("/october-2025-addendum-é.csv", "October 2025 Addendum é"),
                ("/january-2025-addendum-a&amp;b.csv", "January 2025 Addendum A&amp;B"),
            ]
            links.extend(random.sample(edge_cases, min(3, len(edge_cases))))
        
        # Add broken links if requested
        if include_broken:
            broken_links = [
                ("/annual-report.pdf", "Annual Report"),
                ("/errata.txt", "Errata"),
                ("/corrections.docx", "Corrections"),
                ("/general-info.html", "General Information"),
                ("/policy-update.pdf", "Policy Update"),
            ]
            links.extend(random.sample(broken_links, min(2, len(broken_links))))
        
        # Shuffle links for realistic ordering
        random.shuffle(links)
        
        # Generate HTML
        html_parts = [
            "<!DOCTYPE html>",
            "<html>",
            "<head><title>CMS OPPS Quarterly Addenda</title></head>",
            "<body>",
            "<h1>CMS OPPS Quarterly Addenda Updates</h1>",
            "<div class='content'>",
        ]
        
        for href, text in links:
            html_parts.append(f'<a href="{href}">{text}</a><br>')
        
        html_parts.extend([
            "</div>",
            "</body>",
            "</html>"
        ])
        
        return "\n".join(html_parts)
    
    def generate_disclaimer_page(self, 
                               button_text: str = "Accept",
                               include_popup: bool = False) -> str:
        """Generate a synthetic disclaimer page."""
        html_parts = [
            "<!DOCTYPE html>",
            "<html>",
            "<head><title>CMS License Agreement</title></head>",
            "<body>",
            "<h1>CMS Data License Agreement</h1>",
            "<div class='disclaimer'>",
            "<p>By accessing this data, you agree to the terms and conditions.</p>",
            "<p>Please read the disclaimer carefully before proceeding.</p>",
        ]
        
        if include_popup:
            html_parts.extend([
                "<div id='popup' style='display: block;'>",
                "<p>Additional terms may apply.</p>",
                "</div>",
            ])
        
        html_parts.extend([
            f'<button id="accept" onclick="acceptTerms()">{button_text}</button>',
            '<button id="cancel" onclick="cancelTerms()">Cancel</button>',
            "</div>",
            "<script>",
            "function acceptTerms() { window.location.href = '/download'; }",
            "function cancelTerms() { window.history.back(); }",
            "</script>",
            "</body>",
            "</html>"
        ])
        
        return "\n".join(html_parts)
    
    def generate_malformed_html(self) -> str:
        """Generate malformed HTML for error testing."""
        return """
        <html>
        <head><title>Malformed Page</title></head>
        <body>
        <h1>Incomplete
        <a href="/incomplete-link">Incomplete Link
        <div>Unclosed div
        <script>Incomplete script
        </body>
        """
    
    def generate_empty_page(self) -> str:
        """Generate an empty HTML page."""
        return "<html><head><title>Empty</title></head><body></body></html>"
    
    def generate_links_with_special_characters(self) -> str:
        """Generate HTML with special characters in links."""
        return """
        <html>
        <head><title>Special Characters</title></head>
        <body>
        <a href="/addendum-a%20file.csv">Addendum A File</a>
        <a href="/addendum-b+file.xlsx">Addendum B+File</a>
        <a href="/addendum-c_file.zip">Addendum C_File</a>
        <a href="/addendum-d&amp;e.csv">Addendum D&amp;E</a>
        <a href="/addendum-f&lt;g.xlsx">Addendum F&lt;G</a>
        <a href="/addendum-h&gt;i.zip">Addendum H&gt;I</a>
        </body>
        </html>
        """
    
    def save_fixture(self, html_content: str, filename: str, output_dir: Path) -> Path:
        """Save HTML fixture to file."""
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / filename
        file_path.write_text(html_content, encoding='utf-8')
        return file_path
    
    def generate_all_fixtures(self, output_dir: Path) -> Dict[str, Path]:
        """Generate all test fixtures."""
        fixtures = {}
        
        # Generate various page types
        fixtures['quarterly_addenda'] = self.save_fixture(
            self.generate_quarterly_addenda_page(),
            "quarterly_addenda.html",
            output_dir
        )
        
        fixtures['disclaimer_page'] = self.save_fixture(
            self.generate_disclaimer_page(),
            "disclaimer_page.html",
            output_dir
        )
        
        fixtures['disclaimer_popup'] = self.save_fixture(
            self.generate_disclaimer_page(include_popup=True),
            "disclaimer_popup.html",
            output_dir
        )
        
        fixtures['malformed_html'] = self.save_fixture(
            self.generate_malformed_html(),
            "malformed.html",
            output_dir
        )
        
        fixtures['empty_page'] = self.save_fixture(
            self.generate_empty_page(),
            "empty.html",
            output_dir
        )
        
        fixtures['special_characters'] = self.save_fixture(
            self.generate_links_with_special_characters(),
            "special_characters.html",
            output_dir
        )
        
        return fixtures


if __name__ == "__main__":
    """Generate fixtures for testing."""
    generator = OPPSSyntheticHTMLGenerator()
    fixtures_dir = Path("tests/fixtures/opps/html")
    fixtures = generator.generate_all_fixtures(fixtures_dir)
    
    print(f"Generated {len(fixtures)} HTML fixtures:")
    for name, path in fixtures.items():
        print(f"  {name}: {path}")
