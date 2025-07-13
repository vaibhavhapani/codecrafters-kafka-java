import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
         byte[] apiKeyBytes = in.readNBytes(2);
         byte[] apiVersionBytes = in.readNBytes(2);
         byte[] correlationIdBytes = in.readNBytes(4);

         int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
         short apiKey = ByteBuffer.wrap(apiKeyBytes).getShort();
         short apiVersion = ByteBuffer.wrap(apiVersionBytes).getShort();
         int correlationId = ByteBuffer.wrap(correlationIdBytes).getInt();

         boolean isUnsupportedVersion = apiVersion < 0 || apiVersion > 4;

         ByteArrayOutputStream out = new ByteArrayOutputStream();

        out.write(ByteBuffer.allocate(4).putInt(19).array()); // messageSize 6 = 4 for correlation ID + 2 for error code
        out.write(ByteBuffer.allocate(4).putInt(correlationId).array());

         if(isUnsupportedVersion) out.write(ByteBuffer.allocate(2).putShort((short) 35).array());
         else out.write(ByteBuffer.allocate(2).putShort((short) 0).array());

       out.write(new byte[] {
               2,          // compact array length = 1 + 1
               0x00, 0x12, // api_key = 18
               0x00, 0x00, // min_version = 0
               0x00, 0x04, // max_version = 4
               0x00        // tagged_fields = empty
       });


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
