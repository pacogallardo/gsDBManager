'''
        Paco Gallardo 2013

        Merges Scraper Databases
                gsDBManager: https://bitbucket.org/paco_gallardo/agris2scrapper

'''
import mysql.connector
import os.path
import datetime


# main connection configuration
mainServer = [{'host':'localhost', 'database':'gs_1', 'user':'username', 'pw':'password'}]
# connection 2 configuration
slaveServers = [
			{'host':'localhost', 'database':'gs_2', 'user':'username', 'pw':'password'},
			{'host':'localhost', 'database':'gs_3', 'user':'username', 'pw':'password'},
			{'host':'localhost', 'database':'gs_4', 'user':'username', 'pw':'password'}
			]


class DBManager(object):

	__cnx = ''
	__server = ''

	def __init__(self, serverConf):
		super(DBManager, self).__init__()
		self.__cnx = ''
		self.__server = serverConf
	
	def connect(self) : 
		if self.__cnx == '' :
			self.__cnx =  mysql.connector.connect(
				user= self.__server.get('user'), 
				password=self.__server.get('pw'),
				host=self.__server.get('host'), 
				database=self.__server.get('database'))

	def close(self) : 
		self.__cnx.close()

	def executeOne(self, querystring) :
		try :
			cursor = self.__cnx.cursor()
			cursor.execute(querystring)
			result = cursor.fetchone()
			cursor.close
		except mysql.connector.Error as err :
			raise Exception(err)
		return result

	def executeAll(self, querystring) :
		try : 
			cursor = self.__cnx.cursor()
			cursor.execute(querystring)
			result = cursor.fetchall() 
			cursor.close
		except mysql.connector.Error as err :
			raise Exception(err)
		return result

	def executeCount(self, querystring) :
		try :
			cursor = self.__cnx.cursor()
			cursor.execute(querystring)
			number = cursor.fetchone()
			cursor.close
		except mysql.connector.Error as err :
			raise Exception(err)
		return number[0]

	def insert(self, querystring, data) :
		isReturned = ''
		try : 
			cursor = self.__cnx.cursor()
			cursor.execute(querystring,data)
			idReturned = cursor.lastrowid
			self.__cnx.commit()
			cursor.close
		except mysql.connector.Error as err :
			raise Exception(err)
		return idReturned

class Query(object):
	"""docstring for Query"""

	__query = ''
	def __init__(self, id, query, time_query, count, time):
		super(Query, self).__init__()
		self.id = id
		self.query = query
		self.time_query = time_query
		self.count = count
		self.time = time

class Paper(object):
	
	def __init__(self, idRESOURCES, resourceURL, citation, snippetURL, format, isVersionOf, isCiteOf, snippetTitle, snippetDescription, snippetReferences):
		super(Paper, self).__init__()
		self.idRESOURCES = idRESOURCES
		self.resourceURL = resourceURL
		self.citation = citation
		self.snippetURL = snippetURL
		self.format = format
		self.isVersionOf = isVersionOf
		self.isCiteOf = isCiteOf
		self.snippetTitle = snippetTitle
		self.snippetDescription = snippetDescription
		self.snippetReferences = snippetReferences

	def isCite(self) : 
		return not self.isCiteOf == None
			
	def isVersion(self) : 
		return not self.isVersionOf == None
			
class ResourcesManager(object):
	
	def __init__(self):
		super(ResourcesManager, self).__init__()
		
	def getDBQuery(self, cnx, id) :
		result = ''
		try : 
			query = 'SELECT idQUERIES, query, time_query, count, time FROM queries WHERE idQUERIES = %s' % id
			result = cnx.executeOne(query)
		except mysql.connector.Error as err :
			print ('Error: getDBPaper')
		return result

	def getDBQueriesResultByIdQueries(self, cnx, id) : 
		result = ''
		try : 
			query = 'SELECT idRESOURCES, idQUERIES, position, numCitations FROM queries_result WHERE idQUERIES = %s' % id
			result = cnx.executeAll(query)
		except mysql.connector.Error as err :
			print ('Error: getQueriesResult')
		return result

	def getDBQueriesResultByIdResource(self, cnx, id) : 
		result = ''
		try : 
			query = 'SELECT idRESOURCES, idQUERIES, position, numCitations FROM queries_result WHERE idRESOURCES = %s' % id
			result = cnx.executeAll(query)
		except mysql.connector.Error as err :
			print ('Error: getQueriesResult')
		return result

	def getDBResources(self, cnx, id) :
		result = ''
		try : 
			query = 'SELECT A.idRESOURCES, A.resourceURL, A.citation, A.snippetURL, A.format, A.isVersionOf, A.isCiteOf,' \
			' A.snippetTitle, A.snippetDescription, A.snippetReferences ' \
			' FROM resources as A' \
			' INNER JOIN queries_result' \
			' ON queries_result.idRESOURCES = A.idRESOURCES' \
			' WHERE queries_result.idQUERIES = %s' % id 
			result = cnx.executeAll(query)
		except mysql.connector.Error as err :
			print ('Error: getDBResources')
		return result	

	def getDBResourceByResourceURL(self, cnx, resourceURL) :
		result = ''
		try : 
			query = 'SELECT idRESOURCES, resourceURL, citation, snippetURL, format, isVersionOf, isCiteOf, ' \
			'snippetTitle, snippetDescription, snippetReferences FROM resources WHERE isCiteOf IS NULL AND isVersionOf IS NULL AND resourceURL = \'%s\'' % resourceURL
			result = cnx.executeOne(query)
		except mysql.connector.Error as err :
			print ('Error: getDBResources')
		return result

	def getDBResourceByIdResource(self, cnx, idResource) :
		result = ''
		try : 
			query = 'SELECT idRESOURCES, resourceURL, citation, snippetURL, format, isVersionOf, isCiteOf, ' \
			'snippetTitle, snippetDescription, snippetReferences FROM resources WHERE idRESOURCES = %s' % idResource
			result = cnx.executeOne(query)
		except mysql.connector.Error as err :
			print ('Error: getDBResources')
		return result

	def getDBVersions(self, cnx, idResource) :
		result = ''
		try : 
			query = 'SELECT idRESOURCES, resourceURL, citation, snippetURL, format, isVersionOf, isCiteOf, ' \
			'snippetTitle, snippetDescription, snippetReferences FROM resources WHERE isVersionOf = %s' % idResource
			result = cnx.executeAll(query)
		except mysql.connector.Error as err :
			print ('Error: getDBResources')
		return result

	def getDBCites(self, cnx, idResource) :
		result = ''
		try : 
			query = 'SELECT idRESOURCES, resourceURL, citation, snippetURL, format, isVersionOf, isCiteOf, ' \
			'snippetTitle, snippetDescription, snippetReferences FROM resources WHERE isCiteOf = %s' % idResource
			result = cnx.executeAll(query)
		except mysql.connector.Error as err :
			print ('Error: getDBResources')
		return result

	def existQuery(self, cnx, name) : 
		result = ''
		try : 
			query = 'SELECT count(*) FROM queries WHERE query = \'%s\'' % name
			result = cnx.executeCount(query)
		except mysql.connector.Error as err :
			print ('Error: existPaper')
		return result

	def existResource(self, cnx, resourceURL) : 
		result = ''
		try : 
			query = 'SELECT count(*) FROM resources WHERE resourceURL = \'%s\'' % resourceURL
			result = cnx.executeCount(query)
		except mysql.connector.Error as err :
			print ('Error: existPaper')
		return result

	def insertQuery(self, cnx, qObj) : 
		result = ''
		try : 
			query = 'INSERT INTO queries (query, time_query, count, time) VALUES (%s,%s,%s,%s)' 
			data = (qObj.query, qObj.time_query, qObj.count, qObj.time)
			result = cnx.insert(query,data)
		except mysql.connector.Error as err :
			print ('Error: insertQuery')
		return result	

	def insertQueriesResult(self, cnx, idResource, idQuery, position, numCitations) : 
		result = ''
		try : 
			query = 'INSERT INTO queries_result (idRESOURCES, idQUERIES, position, numCitations) VALUES (%s,%s,%s,%s)' 
			data = (idResource,idQuery,position,numCitations)
			result = cnx.insert(query,data)
		except mysql.connector.Error as err :
			print ('Error: insertQuery')
		return result	

	def insertResource(self, cnx, paper) : 
		result = ''
		try : 
			query = 'INSERT INTO resources (resourceURL, citation, snippetURL,' \
				'format, isVersionOf, isCiteOf, snippetTitle, snippetDescription, snippetReferences ) ' \
				' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)' 
			data = (paper.resourceURL, paper.citation, paper.snippetURL, paper.format,
			paper.isVersionOf, paper.isCiteOf, paper.snippetTitle, paper.snippetDescription, paper.snippetReferences )
			result = cnx.insert(query,data)
		except mysql.connector.Error as err :
			print ('Error: insertResource')
		return result	
	
	def maxQuery(self,cnx) : 
		result = 0
		try : 
			query = 'SELECT MAX(idQUERIES) FROM queries'
			result = cnx.executeCount(query)
		except mysql.connector.Error as err :
			print ('Error: countQueries')	
		return int(result)

	def countQueries(self,cnx) : 
		result = 0
		try : 
			query = 'SELECT count(*) FROM queries'
			result = cnx.executeCount(query)
		except mysql.connector.Error as err :
			print ('Error: countQueries')	
		return int(result)

class Log(object):
		
	__file = 'dbManager.log'
	__handler = None
	
	def __init__(self) :
		if self.existFile() : 
			self.rename()
		self.open()
			
	def open(self) :
		self.__handler = open(self.__file, "a")
	
	def close(self) :
		self.__handler.close()
		
	def existFile(self) :
		return os.path.exists(self.__file)

	def rename(self) :
		os.rename(self.__file, self.__file + '_%s' % datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
		
	def write(self, id, query) :
		self.__handler.write('%s - %s\n' % (id,query))
		
def main() : 
	
	try :
		
		log = Log()

		currentIdQuery1 = ''
		currentIdQuery2 = ''

		dbM1 = DBManager(mainServer[0])
		for server in slaveServers :  
			dbM2 = DBManager(server)
			log.write('###', 'database: %s' % server.get('database'))
			manager = ResourcesManager()

			dbM1.connect()
			dbM2.connect()

			numPapers = manager.maxQuery(dbM2)
			
			if numPapers > 0 :
				
				for i in range(1,numPapers+1) : 
					result = manager.getDBQuery(dbM2, i)
					
					
					
					if not result == None :
						print ('%s - %s') % (result[0], result[1].encode('utf-8'))
						log.write(result[0],result[1].encode('utf-8'))
						qObj = Query(result[0],result[1].encode('utf-8'),result[2],result[3],result[4])
						currentIdQuery2 = qObj.id
						idQueryMain = manager.existQuery(dbM1, qObj.query)
						
						if idQueryMain == 0 :
							currentIdQuery1 = manager.insertQuery(dbM1,qObj)
							resources = manager.getDBResources(dbM2, currentIdQuery2)
							for item in resources : 
								resource = Paper(item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7],item[8],item[9])
								
								if not resource.isCite() and not resource.isVersion() :
									currentIdResource1 = manager.insertResource(dbM1,resource)
									qResult =  manager.getDBQueriesResultByIdResource(dbM2,resource.idRESOURCES)
									manager.insertQueriesResult(dbM1,currentIdResource1,currentIdQuery1,qResult[0][2],qResult[0][3])
										
									cites = manager.getDBCites(dbM2,resource.idRESOURCES)
									versions = manager.getDBVersions(dbM2,resource.idRESOURCES)
																
									for iC in cites :
										currentCite = Paper(iC[0],iC[1],iC[2],iC[3],iC[4],iC[5],iC[6],iC[7],iC[8],iC[9])
										currentCite.isCiteOf = currentIdResource1
										currentCiteId = manager.insertResource(dbM1,currentCite)
										qResult = manager.getDBQueriesResultByIdResource(dbM2,currentCite.idRESOURCES)
										manager.insertQueriesResult(dbM1,currentCiteId,currentIdQuery1,qResult[0][2],qResult[0][3])
									for iV in versions : 
										currentVersion = Paper(iV[0],iV[1],iV[2],iV[3],iV[4],iV[5],iV[6],iV[7],iV[8],iV[9])
										currentVersion.isVersionOf = currentIdResource1
										currentVersionId = manager.insertResource(dbM1,currentVersion)
										qResult = manager.getDBQueriesResultByIdResource(dbM2,currentVersion.idRESOURCES)
										manager.insertQueriesResult(dbM1,currentVersionId,currentIdQuery1,qResult[0][2],qResult[0][3])
										
	except mysql.connector.Error as err :
		print (err)
	
	finally:
		log.close()

if __name__ == '__main__':
	main()