import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;

    ClientHandler(Socket clientSocket){
        this.clientSocket = clientSocket;
    }

    public void run() {
        try(BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
            OutputStream out = clientSocket.getOutputStream();){

            while (true) {
                byte[] messageSizeBytes = in.readNBytes(4);
                if (messageSizeBytes.length < 4) break;

                int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
                byte[] requestBody = in.readNBytes(messageSize);

                KafkaRequest request = KafkaRequestParser.parse(requestBody);
                byte[] responseBytes = KafkaResponseBuilder.buildApiVersionsResponse(request);

                out.write(ByteBuffer.allocate(4).putInt(responseBytes.length).array());
                out.write(responseBytes);
                out.flush();
            }
        } catch (IOException e) {
            System.err.println("Client handler error: " + e.getMessage());
        }
    }
}
