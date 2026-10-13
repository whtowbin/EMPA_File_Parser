# EMPA File Parser

Parses Cameca electron microprobe (EPMA) run files (`.txt` or `.qtiDat`) into structured data: the signals and spectrometer crystals used, analysis conditions, and the standards used for calibration. `EMPA Calibration Files/` holds run files from sessions between 2018 and 2022.

```bash
uv run python EMPA_Parse.py path/to/run_file.txt -o run_file.json   # one file to JSON (stdout without -o)
uv run python EMPA_Parse.py path/to/folder --dir                    # every run file in a folder, to CSVs in folder/parsed_csvs/
```

## Status

This is a working research repository, not a finished package. Some files are exploratory analyses or earlier versions kept for reference, and the layout will change as I clean it up.

## License

The code is under the MIT License (see LICENSE).
