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
    line.append(str(h5_file['metadata']['songs'][:1]['artist_7digitalid'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['artist_hotttnesss'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['artist_id'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['artist_latitude'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['artist_location'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['artist_longitude'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['artist_mbid'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['artist_name'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['artist_playmeid'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['release'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['release_7digitalid'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['song_id'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['song_hotttnesss'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['title'][0]))
    line.append(str(h5_file['metadata']['songs'][:1]['track_7digitalid'][0]))
    line.append('\t'.join(h5_file['metadata']['artist_terms'][:]))
    line.append('\t'.join([str(f) for f in h5_file['metadata']['artist_terms_freq'][:]]))
    line.append('\t'.join([str(f) for f in h5_file['metadata']['artist_terms_weight'][:]]))
    line.append('\t'.join([str(f) for f in h5_file['analysis']['bars_start'][:]]))
    line.append('\t'.join([str(f) for f in h5_file['analysis']['bars_confidence'][:]]))
    line.append('\t'.join([str(f) for f in h5_file['analysis']['beats_start'][:]]))
    line.append('\t'.join([str(f) for f in h5_file['analysis']['beats_confidence'][:]]))
    line.append(str(h5_file['analysis']['songs']['danceability'][0]))
    line.append(str(h5_file['analysis']['songs']['end_of_fade_in'][0]))
    line.append(str(h5_file['analysis']['songs']['energy'][0]))
    line.append(str(h5_file['analysis']['songs']['key'][0]))
    line.append(str(h5_file['analysis']['songs']['key_confidence'][0]))
    line.append(str(h5_file['analysis']['songs']['loudness'][0]))
    line.append(str(h5_file['analysis']['songs']['mode'][0]))
    line.append(str(h5_file['analysis']['songs']['mode_confidence'][0]))
    line.append(str(h5_file['analysis']['sections_start'][0]))
    line.append(str(h5_file['analysis']['sections_confidence'][0]))
    line.append(str(h5_file['analysis']['segments_start'][0]))
    line.append(str(h5_file['analysis']['segments_confidence'][0]))
    line.append('\t'.join([str(f) for f in h5_file['analysis']['segments_pitches'][0]]))
    line.append('\t'.join([str(f) for f in h5_file['analysis']['segments_timbre'][0]]))
    line.append(str(h5_file['analysis']['segments_loudness_max'][0]))
    line.append(str(h5_file['analysis']['segments_loudness_max_time'][0]))
    line.append(str(h5_file['analysis']['tatums_start'][0]))
    line.append(str(h5_file['analysis']['tatums_confidence'][0]))
    line.append(str(h5_file['analysis']['songs']['tempo'][0]))
    line.append(str(h5_file['analysis']['songs']['time_signature'][0]))
    line.append(str(h5_file['analysis']['songs']['time_signature_confidence'][0]))
    line.append(str(h5_file['musicbrainz']['songs']['year'][0]))

    print(','.join(line))


def transform_local(path):
    '''
    try:
        with h5py.File(path, 'r+') as h5:
#            return process_h5_file(h5)
            process_h5_file(h5)
    except Exception:
        return []
    '''
    with h5py.File(path, 'r+') as h5:
        process_h5_file(h5)

def rows_to_file(rows):
    # Write some code to save a list of rows into a temporary CSV (or whatever format)
    # for example using pandas.
    transform_local(rows[0])

    
# If using the local volume:
processed = []
chunk_size = 1

for root, dirs, files in os.walk('./data'):
    for f in files:
        processed.append(os.path.join(root, f))
        
        if len(processed) % chunk_size == 0:
            rows_to_file(processed) 
            # upload to s3. make sure to not overwrite the name
            processed = []
            
