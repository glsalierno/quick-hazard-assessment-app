[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/glsalierno/pubchem-toxinfo-cas-retriever/blob/main/LICENSE)
[![Python 3.x](https://img.shields.io/badge/python-3.x-blue.svg)](https://www.python.org/downloads/)
[![MATLAB](https://img.shields.io/badge/MATLAB-R2014b%2B-orange.svg)](https://www.mathworks.com/products/matlab.html)
# PubChem Flash Point Retriever

This repository provides a MATLAB function and Python script to retrieve flash point information from PubChem for given CAS numbers. Focuses on experimental properties; supports batch processing.

## Files
- `fetchFlashPoints.m`: MATLAB function for CAS input and result parsing.
- `get_fp2.py`: Python script for PubChem API queries and JSON output.

## Requirements
- **Python 3.x**: With `requests` library (`pip install requests`).
- **MATLAB**: Base installation (uses system calls and jsondecode).

## Usage
1. Place files in the working directory (ensure Python script is executable/in PATH).
2. In MATLAB: `results = fetchFlashPoints({'50-00-0', '64-17-5'});` (batch example).
3. Results: Cell array with {CAS, flash_points_cell}.

## Notes
- Complies with PubChem API terms (add pauses for large batches).
- Flash points may include multiple values with conditions.
- For issues, open a GitHub issue.

Author: glsalierno  
Date: September 2025  
GitHub: [glsalierno](https://github.com/glsalierno)
