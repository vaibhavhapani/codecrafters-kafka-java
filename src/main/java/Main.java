import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    // 
     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     int port = 9092;
     try {
       serverSocket = new ServerSocket(port);
       // Since the tester restarts your program quite often, setting SO_REUSEADDR
       // ensures that we don't run into 'Address already in use' errors
       serverSocket.setReuseAddress(true);
       // Wait for connection from client.
       clientSocket = serverSocket.accept();

       // First 4 bytes: interprets it as an int32 → total message size (excluding those 4 bytes)
       // Next 4 bytes: interprets it as correlation_id

         BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());

         byte[] messageSizeBytes = in.readNBytes(4);
         int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();

         byte[] apiKey = in.readNBytes(2);
         byte[] apiVersion = in.readNBytes(2);
         byte[] correlationIdBytes = in.readNBytes(4);

         int correlationId = ByteBuffer.wrap(correlationIdBytes).getInt();

         ByteArrayOutputStream out = new ByteArrayOutputStream();
         out.write(ByteBuffer.allocate(4).putInt(correlationId).array());

         clientSocket.getOutputStream().write(out.toByteArray());

     } catch (IOException e) {
       System.out.println("IOException: " + e.getMessage());
     } finally {
       try {
         if (clientSocket != null) {
           clientSocket.close();
         }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       }
     }
  }
}
