package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import android.app.Activity;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.MediaStore.Files;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    static String node_id;
    static String portStr;
    static String myPort;
    private static Uri uri;
    static int successorIndex = -1;
    static int predecessorIndex = -1;
    ArrayList<String> activeNodes = new ArrayList<String>();
    HashMap<String, String> allNodes = new HashMap<String, String>();
    HashMap<String, String> nodeHash  =new HashMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        if(selection.equals("@")){
            for (File file : getContext().getFilesDir().listFiles()){
                if(!file.delete()){
                    System.out.println("File Deletion Failed");
                }
            }
        }else if(selection.equals("*")){
            for (String node: activeNodes){
                String message = "Delete*"+"\n";
                Socket socket;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(allNodes.get(nodeHash.get(node))));
                    BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bWriter.write(message);
                    bWriter.flush();

                    BufferedReader bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    do {
                        // Waiting for Acknowledgement from Server
                    }while(bReader.readLine() == null);
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }else{
            File file = getContext().getFileStreamPath(selection);
            if(file == null || !file.exists()){
                return 0;
            }else{
                if(!file.delete()){
                    System.out.println("File Deletion Failed");
                }
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Set<Map.Entry<String, Object>> set = values.valueSet();
        Iterator itr = set.iterator();

        String key = "";
        String value = "";
        while(itr.hasNext()) {
            Map.Entry entry = (Map.Entry) itr.next();
            value = entry.getValue().toString();
            entry = (Map.Entry) itr.next();
            key = entry.getValue().toString();
        }

        try {
            if(((predecessorIndex == -1) && (successorIndex == -1)) || ((genHash(key).compareTo(activeNodes.get(predecessorIndex)) > 0) && (genHash(key).compareTo(node_id) <= 0))) {
                FileOutputStream outputStream;
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            } else if((activeNodes.indexOf(node_id) == 0) && ((genHash(key).compareTo(activeNodes.get(predecessorIndex)) > 0) || (genHash(key).compareTo(node_id) <= 0))) {
                FileOutputStream outputStream;
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            } else{
                String message = "Insert"+"-"+key+"-"+value+"\n";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(nodeHash.get(activeNodes.get(successorIndex))));
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
                Log.e(TAG, "File write failed");
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        allNodes.put("5554","11108");
        allNodes.put("5556","11112");
        allNodes.put("5558","11116");
        allNodes.put("5560","11120");
        allNodes.put("5562","11124");

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            node_id = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        try {
            activeNodes.add(genHash("5554"));
            nodeHash.put(genHash("5554"), "5554");

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(!portStr.equals("5554")){
            try {
                activeNodes.add(genHash(portStr));
                nodeHash.put(genHash(portStr), portStr);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            String msg = "NodeJoin"+"-"+portStr+"\n";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, allNodes.get("5554"));
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        int count = 0;

        if(selection.equals("@")){
            String[] columns = new String[]{"key", "value"};
            MatrixCursor m = new MatrixCursor(columns);

            for (File file : getContext().getFilesDir().listFiles()){
                byte[] input = new byte[(int)file.length()];
                try {
                    FileInputStream inputStream = new FileInputStream(file);
                    inputStream.read(input);
                    inputStream.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                String value = new String(input);
                m.newRow().add("key", file.getName()).add("value", value);
            }
            return m;
        }else if(selection.equals("*")){
            String[] columns = new String[]{"key", "value"};
            MatrixCursor m = new MatrixCursor(columns);
            for (String node: activeNodes){
                String message = "Query*"+"\n";
                Socket socket;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(allNodes.get(nodeHash.get(node))));
                    BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bWriter.write(message);
                    bWriter.flush();

                    BufferedReader bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String temp;
                    do {
                        // Waiting for Acknowledgement from Server
                        temp = bReader.readLine();
                    }while(temp == null);

                    if(temp.equals("Empty")){
                        continue;
                    }
                    String msgs[] = temp.split("-");
                    for(int i=0; i<msgs.length; i+=2){
                        if(msgs.length == 1){
                            break;
                        }
                        count++;
                        m.newRow().add("key", msgs[i]).add("value", msgs[i+1]);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return m;
        }

        File file = getContext().getFileStreamPath(selection);

        if(file == null || !file.exists()){
            String[] columns = new String[]{"key", "value"};
            MatrixCursor m = new MatrixCursor(columns);

            String message = "Query"+"-"+selection+"\n";
            Socket socket;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(allNodes.get(nodeHash.get(activeNodes.get(successorIndex)))));
                BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bWriter.write(message);
                bWriter.flush();

                BufferedReader bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String temp;
                do {
                    // Waiting for Acknowledgement from Server
                      temp = bReader.readLine();
                }while(temp == null);
                Object[] data = new Object[2];
                data[0] = selection;
                data[1] = temp;
                m.addRow(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return m;
        }else{
            FileInputStream inputStream;
            byte[] input = new byte[(int)file.length()];
            try{
                inputStream = getContext().openFileInput(selection);
                inputStream.read(input);
                inputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File read failed");
            }

            String value = new String(input);
            String[] columns = new String[]{"key", "value"};

            MatrixCursor m = new MatrixCursor(columns);
            m.newRow().add("key", selection).add("value", value);
            return m;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            while(true){
                try {
                    Socket socket = serverSocket.accept();
                    BufferedReader bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String message = bReader.readLine();
                    BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    String[] msgs = message.split("-");

                    if(msgs[0].equals("NodeJoin") && portStr.equals("5554")){
                        bWriter.write("Message Received by Server");
                        bWriter.flush();
                        socket.close();
                        activeNodes.add(genHash(msgs[1]));
                        nodeHash.put(genHash(msgs[1]), msgs[1]);

                        StringBuilder updatedNodes = new StringBuilder();
                        for(String node : activeNodes){
                            updatedNodes.append("-");
                            updatedNodes.append(nodeHash.get(node));
                        }

                        for(String node : activeNodes){
                            if(!nodeHash.get(node).equals("5554")){
                                String msg = "NodeUpdate" + updatedNodes + "\n";
                                Socket clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(allNodes.get(nodeHash.get(node))));
                                BufferedWriter clientBW = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
                                clientBW.write(msg);
                                clientBW.flush();

                                BufferedReader clientBR = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                                do {
                                    // Waiting for Acknowledgement from Server
                                }while(clientBR.readLine() == null);
                                clientSocket.close();
                            }
                        }
                    }else if(msgs[0].equals("NodeUpdate")){
                        bWriter.write("Message Received by Server");
                        bWriter.flush();
                        socket.close();
                        for(int i=1; i<msgs.length;i++){
                            if(!activeNodes.contains(genHash(msgs[i]))){
                                activeNodes.add(genHash(msgs[i]));
                                nodeHash.put(genHash(msgs[i]), msgs[i]);
                            }
                        }
                    }else if(msgs[0].equals("Insert")){
                        bWriter.write("Message Received by Server");
                        bWriter.flush();
                        socket.close();
                        ContentValues cv = new ContentValues();
                        cv.put("key", msgs[1]);
                        cv.put("value", msgs[2]);
                        insert(uri, cv);
                    }else if(msgs[0].equals("Query")){
                        Cursor c = query(uri, null, msgs[1], null, null);
                        if(c.moveToFirst()){
                            bWriter.write(c.getString(1) + "\n");
                            bWriter.flush();
                        }
                        socket.close();
                    }else if(msgs[0].equals("Query*")){
                        int ct = 0;
                        StringBuilder reply = new StringBuilder();
                        Cursor c = query(uri, null, "@", null, null);
                        if(c.isBeforeFirst()){
                            c.moveToFirst();
                        }
                        while(!c.isAfterLast()){
                            ct++;
                            reply.append(c.getString(0));
                            reply.append("-");
                            reply.append(c.getString(1));
                            reply.append("-");
                            c.move(1);
                        }
                        if(ct == 0){
                            bWriter.write("Empty" + "\n");
                            bWriter.close();
                            socket.close();
                        }else{
                            reply.append("\n");
                            bWriter.write(reply.toString());
                            bWriter.close();
                            socket.close();
                        }
                    }else if(msgs[0].equals("Delete*")){
                        bWriter.write("Message Received by Server");
                        bWriter.flush();
                        socket.close();
                        delete(uri, "@", null);
                    }else if(msgs[0].equals("Delete")){
                        bWriter.write("Message Received by Server");
                        bWriter.flush();
                        socket.close();
                        delete(uri, msgs[1], null);
                    }

                    Collections.sort(activeNodes);
                    int currentIndex = activeNodes.indexOf(node_id);
                    successorIndex = currentIndex + 1;
                    predecessorIndex = currentIndex - 1;
                    if(predecessorIndex < 0){
                        predecessorIndex = activeNodes.size() - 1;
                    }
                    if(successorIndex >= activeNodes.size()){
                        successorIndex = 0;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]));
                BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bWriter.write(msgs[0]);
                bWriter.flush();

                BufferedReader bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                do {
                    // Waiting for Acknowledgement from Server
                }while(bReader.readLine() == null);
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}


