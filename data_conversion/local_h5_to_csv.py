'''
Remember to login in using 'aws configure' for uploading csv to s3
'''

import h5py
import os
import boto3 
import tempfile
import pandas as pd

def process_h5_file(h5_file):
    """Do the processing of what fields you'll use here.
     For example, to get the artist familiarity, refer to:
     https://github.com/tbertinmahieux/MSongsDB/blob/master/PythonSrc/hdf5_getters.py
     So we see that it does h5.root.metadata.songs.cols.artist_familiarity[songidx] 
     and it would translate to:
       num_songs = len(file['metadata']['songs'])
       file['metadata']['songs'][:num_songs]['artist_familiarity']
     Since there is one song per file, it simplifies to:
       file['metadata']['songs'][:1]['artist_familiarity']
     I recommend downloading one file, opening it with h5py, and explore/practice
     To see the datatype and shape:
     http://millionsongdataset.com/pages/field-list/
     http://millionsongdataset.com/pages/example-track-description/
     """
    line = []
    line.append(str(h5_file['metadata']['songs'][:1]['artist_familiarity'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['artist_hotttnesss'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['release_7digitalid'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['song_id'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['song_hotttnesss'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['track_7digitalid'][0]))
    line.append(str(h5_file['analysis']['songs']['danceability'][0]))
    line.append(str(h5_file['analysis']['songs']['energy'][0]))
    line.append(str(h5_file['analysis']['songs']['key'][0]))
    line.append(str(h5_file['analysis']['songs']['loudness'][0]))
    line.append(str(h5_file['analysis']['songs']['mode'][0]))
    line.append(str(h5_file['analysis']['songs']['tempo'][0]))
    line.append(str(h5_file['analysis']['songs']['time_signature'][0]))
    line.append(str(h5_file['analysis']['songs']['duration'][0]))
    line.append(str(h5_file['musicbrainz']['songs']['year'][0]))

    return ','.join(line)

def read_and_process(filepaths):
    letter = filepaths[0].split('/')[3]
    filename = letter + '.csv'
    f = open(filename, 'w+')
    for filepath in filepaths:
        try:
            with h5py.File(filepath, 'r') as h5:
                f.write(process_h5_file(h5) + '\n')
        except Exception:
            pass
    f.close()

    s3 = boto3.resource('s3')
    try: 
        s3.meta.client.upload_file(filename, 'millionsongs10605-single', filename)
    except Exception:
        print('letter ' + letter + ' failed')

def main():
    letters = [chr(ord('A') + i) for i in range(26)]

    for letter in letters:
        processed = []
        for root, dirs, files in os.walk('/data/data/' + letter + '/'):
            for f in files:
                processed.append(os.path.join(root, f))
        print(letter + ' round: ')
        print(len(processed))
        print()
        read_and_process(processed)

main()

