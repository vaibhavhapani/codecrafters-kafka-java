import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args){
        System.err.println("Starting Kafka server on port 9092");
        KafkaServer server = new KafkaServer(9092);
        server.start();
    }


}