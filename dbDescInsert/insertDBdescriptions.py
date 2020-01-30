import pycmap
import pandas
import glob
import os


""" Script to import dataset descriptions into database """

#def getDatasetDesc():
# os.path.basename(path)
flist = glob.glob('../../../CMAP_docs/catalog/dataset_descriptions/*.md')

for file in flist:
    f = open(file, 'r')
    str = f.read()
    print(str)



def insertIntoDesc():
    pass


def lineInsert(tableName, columnList ,query, server):
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    insertQuery = """INSERT INTO %s %s VALUES %s """ % (tableName, columnList, query)
    cursor.execute(insertQuery)
    conn.commit()
