"""Use the production HTTP app against isolated Java; no business/model proxy."""
import argparse
import sys
from pathlib import Path
import uvicorn

sys.path.insert(0,str(Path(__file__).resolve().parents[2] / 'agent-service'))
from main import create_app

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--port',type=int,required=True)
    uvicorn.run(create_app(),host='127.0.0.1',port=parser.parse_args().port,log_level='warning')
