import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class KafkaServer {
    private final int port;

    KafkaServer(int port) {
        this.port = port;
    }

    public void start() {
        try(ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Kafka server started on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e){
            System.err.println("Failed to start the server: " + e.getMessage());
        }
    }
}
