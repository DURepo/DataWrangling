import pandas as pd
import csv
import json
import sqlite3

def createCSV(source, dest, cols):
    file = pd.read_csv(source, usecols = cols)
    file.to_csv(dest,index=None, header=True)
    print (dest + 'file created')

def csvtoJson(csvSrc, jsonDest, fields):
    f = open(csvSrc, 'rU', encoding="cp866")

    reader = csv.DictReader(f, fieldnames = fields)
    next(reader)
    #parsing json
    out = json.dumps([row for row in reader])

    f = open(jsonDest, 'w')
    f.write(out)
    print(jsonDest + ' JSON file saved')

#add indentation
def formatJSON(src, dest):
    list = []
    with open(src, 'r') as fp:
        list = json.load(fp)
    with open(dest, 'w') as fp:
        json.dump(list, fp, indent=2)
    print (dest + "readable json complete")


def createTables():
    conn = sqlite3.connect('mydb.db')
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS myschools
                (SchoolID text,
                SchoolName text,
                SchoolMetroType text
                )   
                ''')
    c.execute('''CREATE TABLE IF NOT EXISTS myprojects
                (ProjectID text,
                 SchoolID text,
                ProjectTitle text,
                ProjectSubjectSubcategoryTree text,
                ProjectCost real
                )
                ''')
    print ("tables created!")
    conn.commit()
    conn.close()


def dropTables():
    conn = sqlite3.connect('mydb.db')
    c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS myschools')
    c.execute('DROP TABLE IF EXISTS myprojects')
    print ("tables deleted!")
    conn.commit()
    conn.close()

def insertSchooltoDB(JsonUnformattedsrc, cols):
    conn = sqlite3.connect('mydb.db')
    c = conn.cursor()
    schoollist = []
    #("SchoolID", "SchoolName","SchoolMetroType")
    with open(JsonUnformattedsrc, 'r') as fp:
        schoollist = json.load(fp)
    for s in schoollist:
        c.execute("INSERT INTO myschools VALUES(?,?,?)", (s["SchoolID"], s["SchoolName"], s["SchoolMetroType"]) )
    conn.commit()
    conn.close()
    print ("schools data inserted")

def insertProjectstoDB(JsonUnformattedsrc, cols):
    conn = sqlite3.connect('mydb.db')
    c = conn.cursor()
    projectlist = []
    with open(JsonUnformattedsrc, 'r') as fp:
        projectlist = json.load(fp)
    for p in projectlist:
        c.execute("INSERT INTO myprojects VALUES (?,?,?,?,?)",(p[cols[0]], p[cols[1]], p[cols[2]], p[cols[3]], p[cols[4]]))

    conn.commit()
    conn.close()
    print ("Projects data inserted")

def queriesOnDB():
    conn = sqlite3.connect('mydb.db')
    c = conn.cursor()

    print("Counts of Total records in Schools Table")
    for row in c.execute('SELECT count(*) from myschools') : print (row)
    print ("Counts of Total records in Projects Table")
    for row in c.execute('SELECT count(*) from myprojects'): print (row)

    # read from DB
    print("QUERY #1")
    print("Unique School Metro Types:")
    for row in c.execute('''SELECT Distinct SchoolMetroType FROM myschools '''): print (row)

    print ("QUERY #2")
    print ("Number of Projects accross different schools Per Metro Types")
    for row in c.execute(''' SELECT s.SchoolMetroType, Count(p.ProjectID) FROM myprojects p 
                            INNER JOIN myschools s ON p.SchoolID = s.SchoolID 
                            GROUP BY s.SchoolMetroType'''): print (row)

    print ("QUERY #3")
    print ("Number of Projects in each School sorted in descender order of number of projects, display top 10 results")
    for row in c.execute('''
                        SELECT s.SchoolName, count(p.ProjectID) from myschools s 
                        INNER JOIN myprojects p ON p.SchoolID = s.SchoolID
                        GROUP BY s.SchoolName
                        ORDER BY count(p.ProjectID) desc
                        LIMIT 10
                        ''') : print (row)

    conn.commit()
    conn.close()


# Extract school csv data
def extractSchoolData():
    src = '.\io\Schools.csv'
    dst = '.\SchoolResult_csv.csv'
    cols = ["School ID", "School Name", "School Metro Type"]
    createCSV(src, dst, cols)

# Extract Project csv data
def extractProjectData():
    src = '.\io\Projects.csv'
    dst = '.\ProjectResult_csv.csv'
    cols = ["Project ID", "School ID","Project Title", "Project Subject Subcategory Tree", "Project Cost"]
    createCSV(src, dst, cols)


def main():
    print('executing')
    schoolFields = ("SchoolID", "SchoolName","SchoolMetroType")
    projectFields = ("ProjectID", "SchoolID","ProjectTitle", "ProjectSubjectSubcategoryTree", "ProjectCost")
    # extractSchoolData()
    # extractProjectData()
    # csvtoJson(csvSrc= '.\SchoolResult_csv.csv', jsonDest='.\SchoolResult_json.json', fields=("SchoolID", "SchoolName","SchoolMetroType"))
    # csvtoJson(csvSrc='.\ProjectResult_csv.csv', jsonDest='.\ProjectResult_json.json', fields=("ProjectID", "SchoolID","ProjectTitle", "ProjectSubjectSubcategoryTree", "ProjectCost"))
    # formatJSON(src='.\SchoolResult_json.json', dest='.\SchoolResult_readable_json.json')
    # formatJSON(src='.\ProjectResult_json.json', dest='.\ProjectResult_readable_json.json')
    #
    # #dropTables()
    # createTables()
    # insertSchooltoDB( JsonUnformattedsrc='.\SchoolResult_json.json', cols=schoolFields)
    # insertProjectstoDB(JsonUnformattedsrc='.\ProjectResult_json.json', cols=projectFields)

    queriesOnDB()



if __name__ == "__main__":
    main()