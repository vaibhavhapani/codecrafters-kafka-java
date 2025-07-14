import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args){
        System.err.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            while (true){
                // Wait for connection from client.
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    public static void handleClient(Socket clientSocket) {
        try(BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());) {

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

                    ByteArrayOutputStream res = new ByteArrayOutputStream();

                    res.write(ByteBuffer.allocate(4).putInt(19).array()); // messageSize
                    res.write(ByteBuffer.allocate(4).putInt(correlationId).array());

                    if(isUnsupportedVersion) {
                        res.write(ByteBuffer.allocate(2).putShort((short) 35).array());
                    } else {
                        res.write(ByteBuffer.allocate(2).putShort((short) 0).array());
                    }

                    res.write(new byte[] {
                            2,          // compact array length = 1 + 1
                            0x00, 0x12, // api_key = 18
                            0x00, 0x00, // min_version = 0
                            0x00, 0x04, // max_version = 4
                            0x00,        // tag buffer = empty
                            0x00, 0x00, 0x00, 0x00, // throttle time
                            0x00 // tag buffer
                    });

                    clientSocket.getOutputStream().write(res.toByteArray());
                    clientSocket.getOutputStream().flush(); // Ensure data is sent immediately

                } catch (IOException e) {
                    System.err.println("Error handling request: " + e.getMessage());
                    break; // Exit loop on error
                }
            }

        } catch (IOException e) {
            System.err.println("Client handler error: " + e.getMessage());
        }
    }
}