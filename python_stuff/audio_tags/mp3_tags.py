import eyed3

# Load the MP3 file
ROOT_DIR = "c:/users/guppi/Downloads/"
mp3_file = ROOT_DIR + "masha hima - telka (megacinder dnb remix).mp3"

audiofile = eyed3.load(mp3_file)


# audiofile.tag.artist = "masha hima"
# audiofile.tag.title = "telka (megacinder dnb remix)"
# audiofile.tag.save()

tags = [
    audiofile.tag.artist,
    audiofile.tag.title,
    audiofile.tag.album,
    audiofile.tag.genre,
    audiofile.tag.track_num,
    audiofile.tag.album_artist,
]


for tag in tags:
    print(tag)
