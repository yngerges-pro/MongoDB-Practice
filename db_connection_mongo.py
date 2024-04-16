#-------------------------------------------------------------------------
# AUTHOR: Youstina Gerges
# FILENAME: index_mongo.py, db_connection_mongo.py
# SPECIFICATION: stores data on different Documents with MongoDB
# FOR: CS 4250- Assignment #3
# TIME SPENT: 5+ hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import datetime
from pymongo import MongoClient
import string

def connectDataBase():
     
    # Creating a database connection object using pymongo
    DB_NAME = "Documents"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:

        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected successfully")

def remove_puncuations(term):
    puncs = ["!",",","(",")","-","[","]",";",":","<",">",".","/","?", "@", "#", "$", "%", "^", "&", "*", "_", "~"]
    l = []
    for t in term:
        #print(t)
        if t not in puncs:
            l.append(t)
    newWord = "".join(l)
    #print(newWord)
    return newWord

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # Produce a final document as a dictionary including all the required document fields
    # Value to be inserted
    Document = { 
                "_id": docId,
                "Text": docText,
                "Title": docTitle,
                "Date": docDate,
                "Categories": docCat,
                "Terms": []  # Initialize an empty list for terms
            }

    # Insert the document
    col.insert_one(Document)

    # Create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    term_count = {}  # Dictionary -> {"word": 2, "word2": 1, ...}
    words = docText.lower().split(" ")
    for w in words:
        newW = remove_puncuations(w)
        if newW not in term_count:
            term_count[newW] = 1
        elif newW in term_count:
            term_count[newW] += 1
        else:
            print("did not count correctly.")    

    # Create a list of objects to include full term objects.
    # [{"term", count, num_char}] -> Terms_list Table w/ multiple rows
    
    terms_list = [
        {
            "term": term,
            "count": count,
            "numOfChars": len(term)
        }
        for term, count in term_count.items()
    ]

    # Update the document with the terms list
    col.update_one({"_id": docId}, {"$set": {"Terms": terms_list}})


def deleteDocument(col, docId):
    # Delete the document from the database
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    deleteDocument(col,docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}

    pipeline = [
                {"$unwind" : {"path" : "$Terms"}},
                {"$sort" : {"Terms.term" : 1}}
             ]
    
    items = {}
    messages = col.aggregate(pipeline)
    for msn in messages:
        if msn['Terms']['term'] not in items:
            value = msn['Title']+":"+str(msn['Terms']['count'])
            items[msn['Terms']['term']] = value
        else:
             value = ", "+msn['Title']+":"+str(msn['Terms']['count'])
             items[msn['Terms']['term']] += value

    print(items)



    
   

    
