package org.aueb.ds.model.pubsub;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Connection {

    public Socket socket;
    public ObjectInputStream in;
    public ObjectOutputStream out;

    public Connection(Socket socket) {
        this.socket = socket;
    }

    public Connection(Socket socket, ObjectInputStream in, ObjectOutputStream out) {
        this.socket = socket;
        this.in = in;
        this.out = out;
    }
}
