package com.github.dts.sdk.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class KeepaliveSocket implements Closeable {
    private final InetSocketAddress remoteAddress;
    private Socket socket;

    public KeepaliveSocket(String host, int port) throws UnknownHostException {
        this.remoteAddress = new InetSocketAddress(InetAddress.getByName(host), port);
    }

    public static boolean isConnected(Socket socket) {
        try {
            if (!socket.isConnected()) {
                return false;
            }
            socket.sendUrgentData(0xFF);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public Socket getSocket() {
        return socket;
    }

    public Socket newSocket(int timeout) throws IOException {
        Socket socket = new Socket();
        socket.setKeepAlive(true);
        socket.setOOBInline(false);
        socket.setSoTimeout(timeout);
        socket.connect(remoteAddress, timeout);
        return socket;
    }

    public synchronized boolean isConnected(int timeout) {
        Socket socket = this.socket;
        try {
            if (socket == null) {
                socket = newSocket(timeout);
                if (isConnected(socket)) {
                    this.socket = socket;
                    return true;
                } else {
                    socket.close();
                    return false;
                }
            } else if (isConnected(socket)) {
                return true;
            } else {
                socket.close();
                socket = newSocket(timeout);
                if (isConnected(socket)) {
                    this.socket = socket;
                    return true;
                } else {
                    socket.close();
                    this.socket = null;
                    return false;
                }
            }
        } catch (IOException e) {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                this.socket = null;
            }
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        Socket socket = this.socket;
        if (socket == null) {
            return;
        }
        socket.close();
    }

}