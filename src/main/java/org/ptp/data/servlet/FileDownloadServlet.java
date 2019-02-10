package org.ptp.data.servlet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.logging.Logger;

/*
@WebServlet(description = "Download File From The Server", urlPatterns = { "/downloadServlet" })
*/
public class FileDownloadServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public static int BUFFER_SIZE = 1024 * 100;
    public static final String UPLOAD_DIR = "uploadedFiles";
    public String BASE_DIR = "/";
    public String HDFS_DIR = "/";
    public String CORE_SITE = "/";
    public static Logger logger = Logger.getLogger(FileDownloadServlet.class.getName());

    public void init(ServletConfig config) throws ServletException{

        super.init(config);
        Enumeration<String> names = config.getInitParameterNames();
        int cnt = 0;
        while(names.hasMoreElements()){
            cnt++;
            String n = names.nextElement();
            logger.info("init parameter: " + n);
        }
        logger.info("number of init parameters = " + cnt);

        BASE_DIR = config.getInitParameter("baseDir");
        logger.info("baseDir is " + BASE_DIR);

        if(! BASE_DIR.endsWith(File.separator)){
            BASE_DIR += File.separator;
        }

        HDFS_DIR = config.getInitParameter("hdfsDir");
        logger.info("hdfsDir is " + HDFS_DIR);

        if(! HDFS_DIR.endsWith(File.separator)){
            HDFS_DIR += File.separator;
        }

        CORE_SITE = config.getInitParameter("coresite");


    } // end init


    /***** This Method Is Called By The Servlet Container To Process A 'GET' Request *****/
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        handleRequest(request, response);
    }

    public void handleRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {


        String curRequest = request.getParameter("request");


        if (curRequest.equals("list")) {
            handleListRequest(request, response);
        } else if(curRequest.equals("listhdfs")){
            handleHDFSRequest(request, response);
        } else if(curRequest.equals("gethdfs")){
            handleHDFSDownloadRequest(request, response);
        } else {
            handleDownloadRequest(request, response);
        }

    } // end handleRequest


    public void handleHDFSRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String tld = request.getParameter("tld");
        if(tld == null){
            tld = "";
        } else if(! tld.endsWith(File.separator)){
            tld += File.separator;
        }

        String baseDir = HDFS_DIR + tld;

        Configuration conf = new Configuration();
        logger.info("adding resource: " + CORE_SITE);
        conf.addResource(new Path(CORE_SITE));
        FileSystem hdfs = hdfs = FileSystem.get(conf);

        FileStatus[] fileStatuses = hdfs.listStatus(new Path(baseDir));
        Path[] paths = FileUtil.stat2Paths(fileStatuses);

        String mimeType = "text/html";
        response.setContentType(mimeType);

        response.getWriter().println("<h3>Looking into: " + baseDir + "<br/>");

        for(int x = 0; x < paths.length; x++){
            response.getWriter().println(paths[x].toString() + "<br/>");
        }


    } // end handleHDFSRequest


    public void handleListRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String tld = request.getParameter("tld");
        if(tld == null){
            tld = "";
        } else if(! tld.endsWith(File.separator)){
            tld += File.separator;
        }
        String dir2open = BASE_DIR + tld;
        File f = new File(dir2open);
        String[] l = f.list();
        String mimeType = "text/html";
        response.setContentType(mimeType);

        response.getWriter().println("<h3>Looking into: " + dir2open + "<br/>");
        for(int x = 0; x < l.length; x++){
            response.getWriter().println(l[x] + "<br/>");
        }

    } // end handleListRequest


    public void handleHDFSDownloadRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String tld = request.getParameter("tld");
        if(tld == null){
            tld = "";
        } else if(! tld.endsWith(File.separator)){
            tld += File.separator;
        }

        String fileName = request.getParameter("fileName");
        String downloadStr = HDFS_DIR + tld + fileName;

        Configuration conf = new Configuration();
        logger.info("adding resource: " + CORE_SITE);
        conf.addResource(new Path(CORE_SITE));
        FileSystem hdfs = hdfs = FileSystem.get(conf);

        FileStatus fstat = hdfs.getFileLinkStatus(new Path(downloadStr));
        if(fstat != null){

            String mimeType = "application/octet-stream";
            response.setContentType(mimeType);

            String headerKey = "Content-Disposition";
            String headerValue = String.format("attachment; filename=\"%s\"", fileName);
            response.setHeader(headerKey, headerValue);

            OutputStream outStream = null;
            //FileInputStream inputStream = null;

            FSDataInputStream inputStream = hdfs.open(new Path(downloadStr));

            outStream = response.getOutputStream();
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead = -1;

            /**** Write Each Byte Of Data Read From The Input Stream Write Each Byte Of Data  Read From The Input Stream Into The Output Stream ****/
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }

            inputStream.close();
            outStream.flush();
            outStream.close();


        }


    } // end handleHDFSDownloadRequest


    public void handleDownloadRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String tld = request.getParameter("tld");
        if(tld == null){
            tld = "";
        } else if(! tld.endsWith(File.separator)){
            tld += File.separator;
        }


        /***** Get The Absolute Path Of The File To Be Downloaded *****/
        String fileName = request.getParameter("fileName"),
                applicationPath = getServletContext().getRealPath(""),
                downloadPath = BASE_DIR + tld,
                filePath = downloadPath + File.separator + fileName;

        File file = new File(filePath);
        OutputStream outStream = null;
        FileInputStream inputStream = null;

        if (file.exists()) {

            /**** Setting The Content Attributes For The Response Object ****/
            String mimeType = "application/octet-stream";
            response.setContentType(mimeType);

            /**** Setting The Headers For The Response Object ****/
            String headerKey = "Content-Disposition";
            String headerValue = String.format("attachment; filename=\"%s\"", file.getName());
            response.setHeader(headerKey, headerValue);

            try {

                /**** Get The Output Stream Of The Response ****/
                outStream = response.getOutputStream();
                inputStream = new FileInputStream(file);
                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead = -1;

                /**** Write Each Byte Of Data Read From The Input Stream Write Each Byte Of Data  Read From The Input Stream Into The Output Stream ****/
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outStream.write(buffer, 0, bytesRead);
                }
            } catch(IOException ioExObj) {
                System.out.println("Exception While Performing The I/O Operation?= " + ioExObj.getMessage());
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }

                outStream.flush();
                if (outStream != null) {
                    outStream.close();
                }
            }
        } else {

            /***** Set Response Content Type *****/
            response.setContentType("text/html");

            /***** Print The Response *****/
            response.getWriter().println("<h3>File "+ fileName +" Is Not Present .....!</h3>");
        }
    }

} // end FileDownloadServlet
