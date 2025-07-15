import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args){
        int port = KafkaConstants.PORT;
        System.err.println("Starting Kafka server on port: " + port);
        KafkaServer server = new KafkaServer(port);
        server.start();
    }
}