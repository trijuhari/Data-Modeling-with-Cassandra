session_item_table_create = (
    """
        CREATE TABLE IF NOT EXISTS session_item (
        artist text,
        song text,
         length float,
          session_id int,
          iteminsession int,
          PRIMARY KEY( session_id, iteminsession )
        );
        
    """
 )

user_session_table_create = (
    """
        CREATE TABLE IF NOT EXISTS user_session (
        session_id int,
        user_id int, 
        artist text,
        firstname text,
         lastname text,
         iteminsession int,
          PRIMARY KEY( session_id, user_id )
        );

    """
)

user_song_table_create = (
    """
        CREATE TABLE IF NOT EXISTS user_song (
        user_id int,
         song text,
         firstname text,
         lastname text,
          PRIMARY KEY( (song), user_id )
        );

    """
)




insert_session_item =  (
    """INSERT INTO  session_item  (artist, song, length, session_id, iteminsession)
        VALUES (%s,%s,%s,%s,%s)"""
)

user_session_item =  (
    """INSERT INTO  user_session  (session_id, user_id, artist, firstname, lastname, iteminsession)
        VALUES (%s,%s,%s,%s,%s,%s)"""
)

user_song_item =  (
    """INSERT INTO  user_song  ( user_id, song, firstname, lastname)
        VALUES (%s,%s,%s,%s)""")


delete_user_session = ("""
DROP  TABLE IF EXISTS user_session""")

delete_sesion_item = ("""
DROP  TABLE IF EXISTS session_item""")

delete_user_song = ("""
DROP  TABLE IF EXISTS user_song""")


def kol(line):
    user_session_df = (int(line[5]), int(line[0]), line[-2], line[1], line[2],int(line[4]))
    session_item_df = (line[-2], line[-1], float(line[-5]), int(line[5]), int(line[4]))
    user_song_df = (int(line[0]), line[-1], line[1], line[2] )
    row_postion = [user_session_df, session_item_df, user_song_df]
    return  row_postion

create_table = [ user_session_table_create, session_item_table_create, user_song_table_create]
insert_table  =[ user_session_item, insert_session_item, user_song_item]
drop_table = [delete_user_session, delete_sesion_item, delete_user_song]



