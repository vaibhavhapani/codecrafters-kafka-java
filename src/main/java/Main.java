import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args){
        System.err.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            // Wait for connection from client.
            clientSocket = serverSocket.accept();
            BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());

            // ADD THIS LOOP to handle multiple requests
            while (true) {
                try {
                    // First 4 bytes: interprets it as an int32 -> total message size (excluding those 4 bytes)
                    // Next 4 bytes: interprets it as correlation_id
                    byte[] messageSizeBytes = in.readNBytes(4);

                    // Check if client closed connection
                    if (messageSizeBytes.length < 4) {
                        break; // Client disconnected
                    }

                    byte[] apiKeyBytes = in.readNBytes(2);
                    byte[] apiVersionBytes = in.readNBytes(2);
                    byte[] correlationIdBytes = in.readNBytes(4);

                    int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
                    short apiKey = ByteBuffer.wrap(apiKeyBytes).getShort();
                    short apiVersion = ByteBuffer.wrap(apiVersionBytes).getShort();
                    int correlationId = ByteBuffer.wrap(correlationIdBytes).getInt();

                    // Read remaining bytes of the request (if any)
                    int remainingBytes = messageSize - 8; // 8 bytes already read (apiKey + apiVersion + correlationId)
                    if (remainingBytes > 0) {
                        in.readNBytes(remainingBytes); // Read and discard for now
                    }

                    boolean isUnsupportedVersion = apiVersion < 0 || apiVersion > 4;

                    ByteArrayOutputStream out = new ByteArrayOutputStream();

                    out.write(ByteBuffer.allocate(4).putInt(19).array()); // messageSize
                    out.write(ByteBuffer.allocate(4).putInt(correlationId).array());

                    if(isUnsupportedVersion) {
                        out.write(ByteBuffer.allocate(2).putShort((short) 35).array());
                    } else {
                        out.write(ByteBuffer.allocate(2).putShort((short) 0).array());
                    }

                    out.write(new byte[] {
                            2,          // compact array length = 1 + 1
                            0x00, 0x12, // api_key = 18
                            0x00, 0x00, // min_version = 0
                            0x00, 0x04, // max_version = 4
                            0x00,        // tag buffer = empty
                            0x00, 0x00, 0x00, 0x00, // throttle time
                            0x00 // tag buffer
                    });

                    clientSocket.getOutputStream().write(out.toByteArray());
                    clientSocket.getOutputStream().flush(); // Ensure data is sent immediately

                } catch (IOException e) {
                    System.err.println("Error handling request: " + e.getMessage());
                    break; // Exit loop on error
                }
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}