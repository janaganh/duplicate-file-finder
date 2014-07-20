/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package duplicatefinder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import java.lang.Thread;

//doc class represents unique file - with its path and checksum
//files contents are not directly check byte by byte, but its checksum's are checked
// if 2 files come with same checksum then its taken as a duplicate file
class Doc {
   private String docPath;
   private String docChecksum;
   
   
   public Doc (String docPath,String docChecksum){
       this.docPath = docPath;
       this.docChecksum = docChecksum;
   }
   
   public String getChecksum(){
       return docChecksum;
   }
   
   public String getPath(){
       return docPath;
   }
   //equals method addtionally checks the path is same
   public boolean equals(Object o){
       if (o !=null && o instanceof Doc ){
           Doc doc = (Doc)o; 
           boolean isSame  = false;
           if (this.docChecksum.equals(doc.getChecksum())){
                File f1 = new File(this.docPath);
                File f2 = new File(doc.getPath());
                try {
                 isSame = Files.isSameFile(f1.toPath(), f2.toPath());
                }catch(IOException ex){
                    
                }
                return isSame;
           }                      
       }
       return false;
   }
   
   public String toString(){
       return "["+"Path: "+this.docPath+", Checksum: "+this.docChecksum+"]";
   }
   
   //Doc creator uses faster nio classes and uses CRC32 for generating the checksum
   public static Doc getDocInstance(String filePath) throws IOException {
       FileInputStream inputStream = null;
       try {
            inputStream = new FileInputStream(filePath);
            FileChannel fileChannel = inputStream.getChannel();
            
            int len = (int) fileChannel.size();
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, len);
            CRC32 crc = new CRC32();        
            for (int cnt = 0; cnt < len; cnt++) {
                int i = buffer.get(cnt);
               crc.update(i); 
            }           
            return new  Doc(filePath,Long.toHexString(crc.getValue()));
       }catch(IOException ex){
           throw ex;
       } 
       finally{
           if (inputStream != null){
               inputStream.close();               
           }           
       } 
   }    
}
 //if tasks are not acceppted by the thread pool then its rejected (during shutdown)
 class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {      
        System.out.println(r.toString() + " is rejected");
    }

}
//custom Thread pool executor which can paused and resumed
class PausableThreadPoolExecutor extends ThreadPoolExecutor {
   private boolean isPaused;
   private ReentrantLock pauseLock = new ReentrantLock();
   private Condition unpaused = pauseLock.newCondition();

   public PausableThreadPoolExecutor(int poolSize){
       super(poolSize, poolSize, 2,  TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),new RejectedExecutionHandlerImpl());
       allowCoreThreadTimeOut(true);
   }
   
   protected void beforeExecute(Thread t, Runnable r) {
     super.beforeExecute(t, r);
     pauseLock.lock();
     try {
       while (isPaused) unpaused.await();
     } catch (InterruptedException ie) {
       t.interrupt();
     } finally {
       pauseLock.unlock();
     }
   }

   public void pause() {
     System.out.println("Pausing All Threads");
     pauseLock.lock();
     try {
       isPaused = true;
     } finally {
       pauseLock.unlock();
     }
   }

   public void resume() {
     System.out.println("Resuming All Threads");
     pauseLock.lock();
     try {
         
       isPaused = false;
       unpaused.signalAll();
     } finally {
       pauseLock.unlock();
     }
   }
 }

class SearchException extends Exception {
    public SearchException(String msg){
        super(msg);
    }
}

//main class containg the duplicate search logic
//in summary: starting from the imput folder, all sub folder traversed recursivly
//the files in folder read and checksums are generated
//these checksum put into a concurrent hashmap: checksum as key and Doc object
//as value.
//if hashmap returns object for given key then its duplicate, since an object is
//already existing
public class SearchDuplicate {
    
    private File dir;
    private final ConcurrentHashMap<String,Doc> map = new ConcurrentHashMap<String,Doc>();
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
    private PausableThreadPoolExecutor threadPool;
    private WriteDuplicateFileTask writeTask;
    
    public SearchDuplicate(String searchPath,String outputPath,int threadPoolSize) throws SearchException,
                                                                IOException,InterruptedException {       
         init(searchPath,outputPath, threadPoolSize);         
         System.out.println("Starting Duplicate Search");
         findDuplicates(dir);
         
    }
    //initializes the thread pool
    //initializes the single write file thread
    public void init(String searchPath,String outputPath,int threadPoolSize) throws SearchException,IOException {       
       
         threadPool = new PausableThreadPoolExecutor(threadPoolSize);                    
         dir =  new File(searchPath); 
         if (dir.exists()){
            if (!dir.isDirectory()){
               searchPath = searchPath.substring(0,searchPath.lastIndexOf(File.pathSeparator));
               dir = new File(searchPath);            
            }
         }
         else{
             throw new SearchException("Folder specfified does not exist!");
         }
         writeTask = new WriteDuplicateFileTask(outputPath);
         
    }
    //for concurrency purposes first putifabsent is executed
    //then only the hashmap key checksum test if performed
    //if current doc object is not equal returned Doc object
    //then its taken as duplicate
    public void putDuplicate(Doc doc) throws InterruptedException {                
        map.putIfAbsent(doc.getChecksum(), doc);
        Doc doc2 = map.get(doc.getChecksum());
        //duplicate
        if (!doc2.equals(doc)){
            String msg = "Current File : "+doc.getPath() +" Duplicate file : "+doc2.getPath();            
            queue.put(msg);
            System.out.println("Put: "+ msg);
        }                   
    } 
    
    //for each folder a new task is created
    // its recurively creates new tasks for each new folder
    //if its a file then duplicate check is performed
    public void findDuplicates(File dir) throws InterruptedException {                   
       for (File entry : dir.listFiles()) {               
           if (entry.isDirectory()) {                                //new tasks
              threadPool.execute(new FindDuplicatesTask(entry));
           } else {     
               try {
                   if (entry.length() > 0) {
                    Doc doc = Doc.getDocInstance(entry.getAbsolutePath());
                    putDuplicate(doc);
                   } 
               }catch(IOException ex){                        
                   System.err.println(Thread.currentThread().getName()+": Error accessing file : "+entry.getAbsolutePath()+" Skipping processing!");
               }   
           }
       }                                
    }
    //shuts down threadpool and the writer thread
    public boolean stop() throws SearchException {
        if (!( this.threadPool.isTerminated() ||
                    this.threadPool.isShutdown() ||
                            this.threadPool.isTerminating()) ) {
            System.out.println("Initiate stop");
            this.threadPool.shutdownNow();
            this.writeTask.stop();
            return true;
       }
       return false;
    }
    //pauses threadpool and the writer thread
    public boolean pause() throws SearchException {
       if (!(this.threadPool.isTerminated() || 
                    this.threadPool.isShutdown()|| 
                            this.threadPool.isTerminating())) {
        System.out.println("Initiate pause");
        this.threadPool.pause();
        this.writeTask.pause();
        return true;
       }  
       return false;
    }
    //resumes execution of the  threadpool and the writer thread
    public boolean resume() throws SearchException {
        if (!(this.threadPool.isTerminated() || 
                    this.threadPool.isShutdown()|| 
                            this.threadPool.isTerminating())) {
        System.out.println("Initiate resume");
        this.threadPool.resume();
        writeTask.resume();
        return true;
       }
       return false; 
    }
    //output writer helper class
    class OutputResult {
        private String filePath;
        private RandomAccessFile outputFile ;
        public OutputResult(String filePath ) throws IOException {
            this.filePath = filePath; 
            File file = new File(filePath);
            if (file.exists()){
                file.delete();
            }
            outputFile = new  RandomAccessFile(file, "rw");
            
        }
        public void write(String msg) throws IOException {
            System.out.println(Thread.currentThread().getName()+": Write to output :"+msg);
            if (outputFile == null){
                outputFile = new  RandomAccessFile(filePath, "rw");
                outputFile.seek(outputFile.length());
            }   
            outputFile.writeBytes( String.format(msg+"%n"));
        }   
        
        public void finalize(){
            this.close();
        }
        
        public void close(){
           try { 
            if (outputFile != null){
                outputFile.close();
                outputFile = null;
            }   
           }catch(IOException ex){} 
        }
    } 
    
    //threaded class which writes to file.
    class WriteDuplicateFileTask implements Runnable
    {
        private OutputResult output;
        private Thread writer;
        private volatile boolean threadSuspended;
        
        public WriteDuplicateFileTask(String outputFile) throws IOException {
            output = new OutputResult(outputFile);
            this.start();
        }
        
        public void start() {
            writer = new Thread(this);
            writer.start();
        }
        
        public void run()
        {
            Thread thisThread = Thread.currentThread();
            while (writer == thisThread) 
            {
               try 
               {                    
                    synchronized(this) 
                    {
                        while (threadSuspended && writer == thisThread)
                            wait();
                    }
                    writeOutput();
               }
               catch (IOException e)
                {
                    output.close();
                    throw new RuntimeException(e.getMessage());
                }catch (InterruptedException e)
                {
                    output.close();
                    return;
                }
                              
            }  
        }  
        protected void writeOutput() throws IOException,InterruptedException{
            String msg = queue.take();
            if (msg != null){              
                output.write(msg);
            }
        }
        
        public synchronized void pause() {
          this.threadSuspended = true;
        }
        public synchronized void resume() {
          this.threadSuspended = false;
        }
        public synchronized void stop() {
            writer = null;
            notify();
        }
    } 
    class FindDuplicatesTask implements Runnable {
        private File dir ; 
        
        public FindDuplicatesTask(File dir){
            this.dir = dir; 
        }
        
        @Override
        public void run() {            
            //
            if (Thread.currentThread().isInterrupted()){
                System.out.println(Thread.currentThread().getName()+" Interupted" );
                return;
            }
            System.out.println(Thread.currentThread().getName()+" Start Search  Folder : "+dir.getAbsolutePath());                        
            try {
                findDuplicates(dir);
            }catch(InterruptedException ex) {
                System.out.println(Thread.currentThread().getName()+" Interupted" );                
                return;
            }   
            System.out.println(Thread.currentThread().getName()+" end Search Folder : "+dir.getAbsolutePath());
            return;
        } 
         
         
        public String toString(){
           return "Find Duplicate Task for Folder : "+dir.getAbsolutePath();
        }
    }
    
    public static void main(String args[]) throws Exception{
        
        if (args.length >= 3 ){
         SearchDuplicate s = new  SearchDuplicate(args[0],args[1], Integer.parseInt(args[2]));        
        }
        else{
            System.out.println("Insuffient arguements");
            System.out.println("Please enter <file-path>,<output-path>,<thread pool size>");
        }
        
    }
}





