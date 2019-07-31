# Moving from <0-f>/.../<0-f>/ to <00-ff>/

If the source directory is SOURCE and the target directory is TARGET, do
```
python make_files.py 2 TARGET
python project_map.py SOURCE TARGET
```

This will migrate them from the old format to the new format.
