# Deployment Commands
Commands commonly used during deployment.

## Testing
``python3 -m pytest tests``

## Requirements layer build
### Folder installation and zipping
``python3 -m pip install -r requirements.txt -t ./python && zip -r python.zip ./python/``

## Document building
``pdoc --config='docformat="google"' . --html --force``