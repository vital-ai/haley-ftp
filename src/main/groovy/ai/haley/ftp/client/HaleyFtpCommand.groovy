package ai.haley.ftp.client

import io.vertx.groovy.core.Vertx
import io.vertx.core.AsyncResult;
import io.vertx.groovy.core.Future
import java.util.Map.Entry
import java.util.regex.Matcher
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.vitalai.aimp.domain.AIMPMessage;
import com.vitalai.aimp.domain.Channel
import com.vitalai.aimp.domain.FileQuestion
import com.vitalai.aimp.domain.IntentMessage
import com.vitalai.aimp.domain.MetaQLResultsMessage;
import com.vitalai.aimp.domain.QuestionMessage

import ai.haley.api.HaleyAPI
import ai.haley.api.session.HaleySession
import ai.haley.api.session.HaleyStatus
import ai.vital.domain.FileNode;
import ai.vital.service.vertx3.websocket.VitalServiceAsyncWebsocketClient
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalsigns.model.property.URIProperty
import groovy.json.JsonOutput;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.model.VITAL_Node
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey;

class HaleyFtpCommand {

	static Map cmd2CLI = new LinkedHashMap()

	static String HF = "haleyftp"
	
	static String CMD_HELP = 'help'
	
	static String CMD_PUT = "put"
	 
	static String CMD_GET = "get"
	 
	static String CMD_DEL = "del"
	 
	static String CMD_LS  = "ls"

	static {
		
		def putCLI = new CliBuilder(usage: "${HF} ${CMD_PUT} [options]", stopAtNonOption: false)
		putCLI.with {
			file longOpt: "file", "local file to upload", args: 1, required: false
			d longOpt: "directory", "local directory with files to upload", args: 1, required: false
			s longOpt: "scope", "scope of the files: Public/Private", args: 1, required: true
			c longOpt: "config", "haley client config file", args: 1, required: true
			f longOpt: "filter", "regex filter for input files in directory", args: 1, required: false
		}
		cmd2CLI.put(CMD_PUT, putCLI)
		
		def getCLI = new CliBuilder(usage: "${HF} ${CMD_GET} [options]", stopAtNonOption: false)
		getCLI.with {
			u longOpt: "uri", "remote file uri", args: 1, required: false
			n longOpt: "name", "name of the file to download", args: 1, required: false
			f longOpt: "filter", "search regex filter", args: 1, required: false
			d longOpt: "directory", "directory where files will be saved", args: 1, required: true
//			ow longOpt: "overwrite", "overwrite the output file if exists, if not the filenode uri will be prepended", args: 0, required: false
			c longOpt: "config", "haley client config file", args: 1, required: true
		}
		cmd2CLI.put(CMD_GET, getCLI)
		
		def lsCLI = new CliBuilder(usage: "${HF} ${CMD_LS} [options]", stopAtNonOption: false)
		lsCLI.with {
			c longOpt: "config", "haley client config file", args: 1, required: true
			o longOpt: "offset", "results offset, default 0", args: 1, required: false
			l longOpt: "limit", "results limit, default 10, max 100", args: 1, required: false
			f longOpt: "filter", "search regex filter", args: 1, required: false
		}
		cmd2CLI.put(CMD_LS, lsCLI)
		
		def delCLI = new CliBuilder(usage: "${HF} ${CMD_DEL} [options]", stopAtNonOption: false)
		delCLI.with {
			u longOpt: "uri", "remote file uri", args: 1, required: true
			c longOpt: "config", "haley client config file", args: 1, required: true
		}
		cmd2CLI.put(CMD_DEL, delCLI)
	
	}
	
	static void usage() {
		
		println "usage: ${HF} <command> [options] ..."
		
		println "usage: ${HF} ${CMD_HELP} (prints usage)"
		
		for(Entry e : cmd2CLI.entrySet()) {
			e.value.usage()
		}
		
	}

	
	String[] args
	
	
	String endpointURL
	String appID
	VitalApp app
	String username
	String password
	
	Vertx vertx
	
	HaleyAPI haleyAPI
	HaleySession haleySession
	Channel loginChannel
	
	String cmd = null
	
	Integer offset = 0
	Integer limit = 10
	String filter
	
	
	String fileNodeURI
	String name
	File outputDir
	Boolean overwrite = null
	
	
	List sourceFiles = []
	String scope
	
	int filesOK = 0
	int filesSkipped = 0
	int filesERROR = 0
	
	List fileNodes = []
	
	public static void main(String[] args) {
		
		new HaleyFtpCommand(args).run()
		
	}
	
	HaleyFtpCommand(String[] args) {
		this.args = args
	} 
	
	def run() {
		
		cmd = args.length > 0 ? args[0] : null
		
		boolean printHelp = args.length == 0 || cmd == CMD_HELP
			
		if(printHelp) {
			usage();
			return;
		}
			
			
		String[] params = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0]
			
		def cli = cmd2CLI.get(cmd)
			
		if(!cli) {
			System.err.println "unknown command: ${cmd}"
			usage()
			return
		}
			
		def options = cli.parse(params)
		if(!options) {
			return
		}
		
		if(cmd == CMD_LS) {
			
			String offsetString = options.o ? options.o : null
			if(offsetString) {
				try {
					offset = Integer.parseInt(offsetString)
				} catch(Exception e) {
					error "Invalid offset param: ${offsetString} - ${e.localizedMessage}"
					return
				}
				
				if(offset < 0) {
					error("offset must be >= 0: " + offset)
					return
				}
				
			}
			println "offset: ${offset}"
			
			String limitString = options.l ? options.l : null
			if(limitString) {
				try {
					limit = Integer.parseInt(limitString)
				} catch(Exception e) {
					error "Invalid limit param: ${limitString} - ${e.localizedMessage}"
					return
				}
				
				if(limit < 1 || limit > 100) {
					error("limit must be in range [1, 100]: " + limit)
					return
				}
			}
			println "limit: ${limit}"
			
			filter = options.f ? options.f : null
			println "filter: ${filter ? filter : ''}"
			
			
		} else if(cmd == CMD_GET) {
		
			outputDir = new File(options.d)
			name = options.n ? options.n : null
			filter = options.f ? options.f : null
//			overwrite = options.ow ? true : false
			fileNodeURI = options.u ? options.u : null
			
			int c = 0
			if(name) c++
			if(filter) c++
			if(fileNodeURI) c++
			if(c != 1) {
				error("Exactly one of --name/--uri/--filter params required, passed: " + c)
				return
			}
			
			if(!outputDir.exists()) {
				error("output directory does not exist: " + outputDir.absolutePath)
				return
			}
			if(!outputDir.isDirectory()) {
				error("output path is not a directory: " + outputDir.absolutePath)
				return
			}
			
			println "name: ${name}"
			println "filter: ${filter}"
			println "fileNodeURI: ${fileNodeURI}"
//			println "overwrite: ${overwrite}"
			println "output directory: ${outputDir.absolutePath}"
			
		} else if(cmd == CMD_PUT) {
		
			String f = options.file ? options.file : null
			String d = options.d ? options.d : null
			if(f && d) {
				error('--file and --directory options are mutually exclusive')
				return
			}
			if(!f && !d) {
				error("exactly one of --file or --directory params is required")
				return
			}
			
			String s = options.s ? options.s : null
			if(!s) {
				error("No --scope param")
				return
			} 
			if(!( 'Public'.equals(s) || 'Private'.equals(s))) {
				error("Scope param must be either Public or Private: " + s)
				return
			}
			
			scope = s
			
			if(f) {
				File file = new File(f)
				if(!file.isFile()) {
					error("File path is not a file: " + file.absolutePath)
					return
				}
				println "input file: ${file.absolutePath}"
				sourceFiles.add(file)
			} else {
			
				filter = options.f ? options.f : null
			
				Pattern pattern = Pattern.compile(filter)
				println "filter: ${filter}"
			
				File dir = new File(d)
				if(!dir.isDirectory()) {
					error("Path is not directory: " + dir.absolutePath)
					return
				}
				for(File x : dir.listFiles()) {
					if(x.isFile()) {
						if(pattern) {
							if( ! pattern.matcher(x.name).matches() ) {
								println "file filtered out: ${x.absolutePath}"
								continue
							}
						}
						sourceFiles.add(x)
						println "Input file: ${x.absolutePath}"
					}
				}
				if(sourceFiles.size() == 0) {
					error "No files found in directory: ${dir.absolutePath}"
					return
				}
			}
		
		} else if(cmd == CMD_DEL) {
		
			fileNodeURI = options.u ? options.u : null
			
			if(!fileNodeURI) {
				error "No --uri param"
				return
			}
		
			println "fileNodeURI: ${fileNodeURI}"
			
		}
		
		
		String configPath = options.c ? options.c : null
		
		if(!configPath) {
			System.err.println "no --config param"
			return
		}
		
		File configFile = new File(configPath)
		if(!configFile.isFile()) {
			error "config file not found: ${configFile.getAbsolutePath()}"
			return
		}
		
		println "loading config file..."
		
		Config config = ConfigFactory.parseFile(configFile)
		
		endpointURL = config.getString("endpointURL")
		appID = config.getString("appID")
		username = config.getString("username")
		password = config.getString("password")
		
		String p = ''
		for(int i = 0 ; i < password.length(); i++) {
			p += '*'
		}
		
		println "appID: ${appID}"
		println "endpointURL: ${endpointURL}"
		println "username: ${username}"
		println "password: ${p}"
		
		initClient()
		
	}
	
	def initClient() {
		
		app = VitalApp.withId(appID)
		
		vertx = Vertx.vertx()
				
		VitalServiceAsyncWebsocketClient websocketClient = new VitalServiceAsyncWebsocketClient(vertx, app, 'endpoint.', endpointURL)
		
		websocketClient.connect({ Throwable exception ->
			
			if(exception) {
				exception.printStackTrace()
				return
			}
			
			haleyAPI = new HaleyAPI(websocketClient)
			
			onHaleyAPIConnected()
			
		}, {Integer attempts ->
			System.err.println("FAILED, attempts: ${attempts}")
		})
		
	}
	
	def onHaleyAPIConnected() {
		
		haleyAPI.openSession() { String errorMessage,  HaleySession session ->
			
			haleySession = session
			
			if(errorMessage) {
				throw new Exception(errorMessage)
			}
			
			println "Session opened ${session.toString()}"
			
			println "Sessions: " + haleyAPI.getSessions().size()
			
			onSessionReady()
			
		}
		
	}
	
	def onSessionReady() {
		
		haleyAPI.authenticateSession(haleySession, username, password) { HaleyStatus status ->
			
			println "auth status: ${status}"

			if(!status.ok()) {
				System.exit(-1)
				return
			}
			
			println "session: ${haleySession.toString()}"
			
			onHaleyReady()
			
		}
		
	}
	
	def onHaleyReady() {
		
		haleyAPI.listChannels(haleySession) { String error, List<Channel> channels ->
			
			for(Channel channel : channels) {
				
				if(channel.name?.toString() == username) {
					loginChannel = channel
				}
				
			}
			
			if(loginChannel == null) {
				error("Own login channel not found: ${username}")
				return
			}
			
			onLoginChannelReady()
			
		}
	}
			
	def onLoginChannelReady() {
	
		if(cmd == CMD_LS) {
			
			listFiles()
			
		} else if(cmd == CMD_GET) {
		
			getFiles()
		
		} else if(cmd == CMD_PUT) {
		
			putNextFile()
			
		} else if(cmd == CMD_DEL) {
		
			deleteFile()
		
		} else {
		
			error "Unhandled command: ${cmd}"
		
		}		
		
	}
	
	def putNextFile() {
		
		if(sourceFiles.size() == 0) {
			
			println "DONE, files uploaded ${filesOK} / errors: ${filesERROR}"
			
			System.exit(-1)
			
			return
		}
		
		File sourceFile = sourceFiles.remove(0)
		
		println "Uploading file ${sourceFile.absolutePath}"
		
		IntentMessage intentMsg = new IntentMessage()
		intentMsg.generateURI((VitalApp) null)
		intentMsg.channelURI = loginChannel.URI
		intentMsg.intent = 'fileupload'
		intentMsg.propertyValue = scope
		

		haleyAPI.sendMessageWithRequestCallback(haleySession, intentMsg, [], { ResultList msgRL ->
			
			AIMPMessage msg = msgRL.first()
			
			if(! ( msg instanceof QuestionMessage) ) {
				println("Ignoring message of type: " + msg.getClass());
				return true;
			}
			
			List<FileQuestion> fileQuestions = msgRL.iterator(FileQuestion.class).toList()
			
			if(fileQuestions.size() == 0) {
				throw new Exception(HaleyStatus.error("No file question received to file upload intent"))
				return false;
			}
			
			FileQuestion fq = fileQuestions.get(0)
			
			println ("file question received");
 			
			haleyAPI.uploadFile(haleySession, msg, fq, sourceFile) { HaleyStatus uploadStatus ->
				
				if(uploadStatus.isOk()) {
	
					println "File uploaded successfully: ${sourceFile.absolutePath} - fileNodeURI: ${uploadStatus.result.URI}"
					
					filesOK++			
						
				} else {
				
					System.err.println "Error when uploading file: ${sourceFile.absolutePath} - ${uploadStatus.errorMessage}"
				
					filesERROR++
					
				}
				
				putNextFile()
				
			}
			
			return false;
			
		}, { HaleyStatus sendStatus->
			 
			if( ! sendStatus.isOk() ) {
				throw new Exception("Error when sending file upload intent: " + sendStatus.errorMessage)
			}
			
			//message sent, wait for reply
			println("file upload intent sent")
			
		})
		

		
	}
	
	
	
	def listFiles() {
		
		Map fParams = [
			sortColumn: '',
			sortDirection: '',
			offset: offset,
			limit: limit,
			filter: filter
		]
		
		
		IntentMessage intentMessage = new IntentMessage()
		intentMessage.generateURI(app)
		intentMessage.intent = 'table'
		intentMessage.propertyValue = 'files ' + JsonOutput.toJson(fParams)
		intentMessage.channelURI = loginChannel.URI
		
		haleyAPI.sendMessageWithRequestCallback(haleySession, intentMessage, [], { ResultList msgRL -> 
		
			AIMPMessage msg = msgRL.first()
			
			if(!(msg instanceof MetaQLResultsMessage)) {
				println "ignoring non-results message: " + msg.getClass()
				return true
			}
			
			String status = msg.status
			if(!'ok'.equalsIgnoreCase(status)) {
				String errorMsg = msg.statusMessage
				if(!errorMsg) errorMsg = 'unknown error'
				error "error when listing files: " + errorMsg
				return false
			}
			
			int i = 0 ; 
			
			
			println "total files: " + msg.totalResults + ",${filter ? ('filter: ' + filter ) : ''} offset: ${offset}, limit: ${limit}" 
			
			for(FileNode fn : msgRL.iterator(FileNode.class)) {
				
				i++
				println "${offset + i} ${fn.URI} ${fn.name} ${fn.fileScope} ${fn.timestamp != null ? new Date(fn.timestamp.longValue()).toGMTString() : '-'}"
				println "\tURL: " + haleyAPI.getFileNodeDownloadURL(haleySession, fn)
			}
			
			println "DONE"
			
			System.exit(-1)
			
			return false
				
		}, {HaleyStatus sendStatus ->
			
			if(!sendStatus.isOk()) {
				error("error when sending list files request: ${sendStatus.errorMessage}")
				return
			}
			
			println "list files request sent"
			
		})
		
	}
	
	def deleteFile() {
		
		IntentMessage intentMessage = new IntentMessage()
		intentMessage.generateURI(app)
		intentMessage.intent = 'deleteobject'
		intentMessage.propertyValue = 'file ' + fileNodeURI
		intentMessage.channelURI = loginChannel.URI
		
		haleyAPI.sendMessageWithRequestCallback(haleySession, intentMessage, [], { ResultList msgRL ->

			AIMPMessage msg = msgRL.first()
			if(!(msg instanceof MetaQLResultsMessage)) {
				println "ignoring non-results message: " + msg.getClass()
				return true
			}
			
			String status = msg.status
			if(!'ok'.equalsIgnoreCase(status)) {
				String errorMsg = msg.statusMessage
				if(!errorMsg) errorMsg = 'unknown error'
				error "error when listing files: " + errorMsg
				return false
			}
			
			println "File deleted successfully: ${fileNodeURI}"
			
			println "DONE"
			System.exit(-1)
			
			return false
					
		}, { HaleyStatus sendStatus ->
			
			if(!sendStatus.isOk()) {
				error("error when sending delete file request: ${sendStatus.errorMessage}")
				return
			}
			
			println "delete file request sent"
			
			
		})		
	}
	
	def getFiles() {
		
		println "Getting files..."
		
		if( fileNodeURI ) {
			
			IntentMessage intentMessage = new IntentMessage()
			intentMessage.generateURI(app)
			intentMessage.intent = 'details'
			intentMessage.propertyValue = 'file ' + fileNodeURI
			intentMessage.channelURI = loginChannel.URI
			
			haleyAPI.sendMessageWithRequestCallback(haleySession, intentMessage, [], { ResultList msgRL ->
			
				AIMPMessage msg = msgRL.first()
				
				if(!(msg instanceof MetaQLResultsMessage)) {
					println "ignoring non-results message: " + msg.getClass()
					return true
				}
				
				String status = msg.status
				if(!'ok'.equalsIgnoreCase(status)) {
					String errorMsg = msg.statusMessage
					if(!errorMsg) errorMsg = 'unknown error'
					error "error when listing files: " + errorMsg
					return false
				}
				
				int i = 0 ;
				
				fileNodes = msgRL.iterator(FileNode.class).toList()
				if(fileNodes.size() == 0) {
					error "file not found: ${fileNodeURI}"
					return
				}

				downloadNextFileNode()
								
				return false
					
			}, {HaleyStatus sendStatus ->
				
				if(!sendStatus.isOk()) {
					error("error when sending list files request: ${sendStatus.errorMessage}")
					return
				}
				
				println "list files request sent"
				
			})
			
		} else {
		
		
			Map fParams = [
				sortColumn: '',
				sortDirection: '',
				offset: 0,
				limit: 1000,
				filter: filter,
				name: name
			]
			
			
			IntentMessage intentMessage = new IntentMessage()
			intentMessage.generateURI(app)
			intentMessage.intent = 'table'
			intentMessage.propertyValue = 'files ' + JsonOutput.toJson(fParams)
			intentMessage.channelURI = loginChannel.URI
			
			haleyAPI.sendMessageWithRequestCallback(haleySession, intentMessage, [], { ResultList msgRL ->
			
				AIMPMessage msg = msgRL.first()
				
				if(!(msg instanceof MetaQLResultsMessage)) {
					println "ignoring non-results message: " + msg.getClass()
					return true
				}
				
				String status = msg.status
				if(!'ok'.equalsIgnoreCase(status)) {
					String errorMsg = msg.statusMessage
					if(!errorMsg) errorMsg = 'unknown error'
					error "error when listing files: " + errorMsg
					return false
				}
				
				int i = 0 ;
				
				fileNodes = msgRL.iterator(FileNode.class).toList()
				println "files found: " + fileNodes.size()

				if(fileNodes.size() == 0) {
					error "no files found " + (name ? ('with name: ' + name) : ('for filter: ' + filter))
					return
				}
				
				
				downloadNextFileNode()
								
				return false
					
			}, {HaleyStatus sendStatus ->
				
				if(!sendStatus.isOk()) {
					error("error when sending list files request: ${sendStatus.errorMessage}")
					return
				}
				
				println "list files request sent"
				
			})
			
		
		}
		
	}
	
	static void error(String m) {
		System.err.println "ERROR: ${m}"
		System.exit(-1)
	}

	def downloadNextFileNode() {
		
		
		if(fileNodes.size() == 0) {
			println "Downloaded ${filesOK} files, errors: ${filesERROR}"
			println ("DONE")
			System.exit(-1)
			return
		}
		
		FileNode fileNode = fileNodes.remove(0)
		
		String downloadURL = haleyAPI.getFileNodeDownloadURL(haleySession, fileNode)

		File targetFile = new File(outputDir, fileNode.name.toString())
		
		if(targetFile.exists()) {
			println "File already exists: " + targetFile.name + " saving with prepended URI"
			targetFile = new File(outputDir, URLEncoder.encode(fileNode.URI, 'UTF-8') + '__' + fileNode.name)
		}
		
		vertx.executeBlocking({ Future future ->
			
			InputStream inputStream = null
			OutputStream outputStream = null
			try {
				inputStream = new URL(downloadURL).openStream()
				outputStream = new FileOutputStream(targetFile)

				long ts = System.currentTimeMillis()
				println "Saving file ${fileNode.URI} ${fileNode.fileLength} bytes -> ${targetFile.getAbsolutePath()}"
								
				IOUtils.copy(inputStream, outputStream)
				println "File saved ${System.currentTimeMillis() - ts} ms"
				filesOK++
					
			} catch(Exception e) {
				System.err.println("Error when downloading file ${fileNode.URI}: ${e.localizedMessage}")
				filesERROR++
			} finally {
				IOUtils.closeQuietly(inputStream)
				IOUtils.closeQuietly(outputStream)
			}
			
			future.complete()
			 
		}, { AsyncResult<Void> res -> 
		
			downloadNextFileNode()	
		
		})
				
		
	}
		
}
