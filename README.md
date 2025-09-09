# EDGAR Text Analyzer

A general-purpose tool for downloading and analyzing SEC EDGAR filings (e.g., 10-K reports).  
It allows researchers and practitioners to automatically collect filings by SIC code and run text analysis on them.

In this case, the tool has been applied to study **narrative evidence of monetary policy** in insurers' 10-Ks, but it can be adapted to other research questions and keywords.

## Features
- Download company lists by SIC code  
- Download 10-K filings directly from EDGAR  
- Flexible text analysis using customizable dictionaries (regex-based)  
- Parallel processing for large-scale analysis  
- Outputs detailed hit-level data and aggregated counts in CSV format  

## Usage
1. Generate a company list by SIC code (`1. Create List of All Relevant Companies`)  
2. Download 10-K filings (`2. Download 10-K Filings`)  
3. Run text analysis (`3. Run the Analysis`)  

All outputs will be stored in your chosen project directories.

## Example Application
- Measuring how often insurers mention **monetary policy terms** such as "interest rates", "Federal Reserve", or "yield curve" in their 10-Ks.  
- The output can be used for text analysis of 10-K filings.

## License
This project is licensed under the [MIT License](LICENSE).
