import java.io.*;
import java.net.*;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;
import java.util.Set;

public class BootstrapServer {
    static Map<Integer, String> pairs = new TreeMap<>();
    static List<NameServer> nameServerList = new ArrayList<>();
    static DoubleLinkedList nameServerIDList = new DoubleLinkedList();
    static NameServer successorNameServer;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Error!! Enter Config file");
            return;
        }

        final String config = args[0];
        final List<String> fileContents = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(config))) {
            String readMessage;
            while ((readMessage = reader.readLine()) != null) {
                fileContents.add(readMessage);
            }

            int serverID = Integer.parseInt(fileContents.get(0));
            fileContents.remove(0);
            int serverPort = Integer.parseInt(fileContents.get(0));
            fileContents.remove(0);

            String[] arg;
            for (var i : fileContents) {
                arg = i.split(" ");
                pairs.put(Integer.parseInt(arg[0]), arg[1]);
            }
            System.out.println("BootStrap Server started");
            System.out.println("Listening on port: " + serverPort);

            Thread uiThread = new Thread(new UserInteractionThread());
            uiThread.start();

            try (ServerSocket socket = new ServerSocket(serverPort)) {
                nameServerIDList.add(0);
                NameServer ns = new NameServer(serverID, InetAddress.getLocalHost().getHostAddress(), serverPort);
                nameServerList.add(ns);
                while (true) {
                    Socket cSocket = socket.accept();
                    BufferedReader read = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
                    PrintWriter write = new PrintWriter(cSocket.getOutputStream(), true);
                    String[] nsLine = read.readLine().split(" ");

                    if (nsLine[0].equalsIgnoreCase("enter")) {
                        enter(InetAddress.getLocalHost().getHostAddress(), serverID, serverPort, read, write, nsLine,
                                cSocket.getInetAddress().getHostAddress());
                        System.out.println("New Name Server connected: " + cSocket.getInetAddress());
                    } else if (nsLine[0].equalsIgnoreCase("acquire")) {
                        acquire(read, write, nsLine);
                        System.out.println("Pairs sent successfully to new NameServer");
                    } else if (nsLine[0].equalsIgnoreCase("exit")) {
                        exit(read, write, nsLine, cSocket.getInetAddress().getHostAddress());
                        System.out.println("Name Server Exited");
                    } else if (nsLine[0].equalsIgnoreCase("handover")) {
                        System.out.println(handover(read, write, nsLine));
                        for (var i : pairs.entrySet()) {
                            System.out.println(i.getKey() + " : " + i.getValue());
                        }
                    }
                    cSocket.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String enter(String serverIP, int serverID, int serverPort, BufferedReader read, PrintWriter write,
            String[] nsLine, String nsIP) {
        int nsID = Integer.parseInt(nsLine[1]);
        int nsPort = Integer.parseInt(nsLine[2]);
        NameServer ns = new NameServer(nsID, nsIP, nsPort);
        nameServerList.add(ns);
        nameServerIDList.add(nsID);
        if (successorNameServer == null) {
            successorNameServer = new NameServer(ns.getNSID(), ns.getNSIP(), ns.getNSPort());
        } else if (successorNameServer.getNSID() > ns.getNSID()) {
            successorNameServer = new NameServer(ns.getNSID(), ns.getNSIP(), ns.getNSPort());
        }
        Node n = nameServerIDList.getNode(nsID);
        int predecessorID = n.previous.ID;
        String predecessorIP = " ";
        int predecessorPort = -1;
        for (var i : nameServerList) {
            if (i.getNSID() == predecessorID) {
                predecessorIP = i.getNSIP();
                predecessorPort = i.getNSPort();
                break;
            }
        }

        int successorID, successorPort = -1;
        String successorIP = " ";
        if (n.next != null) {
            successorID = n.next.ID;
            for (var i : nameServerList) {
                if (i.getNSID() == successorID) {
                    successorIP = i.getNSIP();
                    successorPort = i.getNSPort();
                }
            }
        } else {
            successorID = serverID;
            successorIP = serverIP;
            successorPort = serverPort;
        }
        write.println(predecessorID + " " + predecessorIP + " " + predecessorPort + " " + successorID + " "
                + successorIP + " " + successorPort);
        return "Name Server added";
    }

    public static String acquire(BufferedReader read, PrintWriter write, String[] nsLine) {
        int nsID = Integer.parseInt(nsLine[1]);

        List<Integer> temp = new ArrayList<>();
        for (var i : BootstrapServer.pairs.entrySet()) {
            int key = i.getKey();
            if (key <= nsID && key != 0) {
                write.println(key + " " + i.getValue());
                temp.add(key);
            }
        }
        for (int i : temp) {
            BootstrapServer.pairs.remove(i);
        }
        return ("Key-value pairs shared to server : " + nsID);
    }

    public static String exit(BufferedReader read, PrintWriter write, String[] nsLine, String nsIP) {
        int nsID = Integer.parseInt(nsLine[1]);
        int nsPort = Integer.parseInt(nsLine[2]);
        NameServer ns = new NameServer(nsID, nsIP, nsPort);
        BootstrapServer.nameServerList.remove(ns);
        nameServerIDList.delete(nsID);

        if (successorNameServer.getNSID() == nsID) {
            successorNameServer = new NameServer(Integer.parseInt(nsLine[3]), nsLine[4], Integer.parseInt(nsLine[5]));
        }
        return ("Name Server removed");
    }

    public static String handover(BufferedReader read, PrintWriter write, String[] nsLine)
            throws NumberFormatException, IOException {
        int nsID = Integer.parseInt(nsLine[1]);
        String bsLine;
        while ((bsLine = read.readLine()) != null) {
            String[] b = bsLine.split(" ");
            BootstrapServer.pairs.put(Integer.parseInt(b[0]), b[1]);
            System.out.println(Integer.parseInt(b[0]) + " " + BootstrapServer.pairs.get(Integer.parseInt(b[0])));
        }
        return ("Key-value pairs received from server : " + nsID);
    }
}

class UserInteractionThread extends Thread {
    public void run() {
        try (BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                System.out.print("BootstrapServer> ");
                String readLine = userInput.readLine();

                String[] arg = readLine.split(" ");

                if ("lookup".equalsIgnoreCase(arg[0])) {
                    int lKey = Integer.parseInt(arg[1]);
                    Set<Integer> i = BootstrapServer.pairs.keySet();
                    if (i.contains(lKey)) {
                        System.out.println("Key found at Bootstrap server: " + BootstrapServer.pairs.get(lKey));
                    } else {
                        String successorIP = BootstrapServer.successorNameServer.getNSIP();
                        int successorPort = BootstrapServer.successorNameServer.getNSPort();
                        try (Socket socket = new Socket(successorIP, successorPort)) {
                            BufferedReader readSucc = new BufferedReader(
                                    new InputStreamReader(socket.getInputStream()));
                            PrintWriter writeSucc = new PrintWriter(socket.getOutputStream(), true);
                            writeSucc.println("lookup " + lKey);
                            String[] succOutput = readSucc.readLine().split(" ");
                            if (succOutput[0].equals("null")) {
                                System.out.println("Key not found");
                            } else {
                                System.out.println("Key Found !!");
                                System.out.print("Servers Traversed: bootstrap: 0 > ");
                                for (int j = 0; j <= succOutput.length - 2; j++) {
                                    System.out.print("server: " + succOutput[j] + " > ");
                                }
                                System.out.println(succOutput[succOutput.length - 1]);
                            }

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    continue;
                }

                else if ("insert".equalsIgnoreCase(arg[0])) {
                    int iKey = Integer.parseInt(arg[1]);
                    if (arg.length != 3) {
                        System.out.println("ERROR!! Both Key and value");
                        continue;
                    }
                    String iValue = arg[2];
                    int tail = BootstrapServer.nameServerIDList.getTail();
                    if (iKey > tail && iKey < 1024) {

                        if (BootstrapServer.pairs.containsKey(iKey)) {
                            System.out.println("Key already present in Bootstrap server");
                        } else {
                            BootstrapServer.pairs.put(iKey, iValue);
                            System.out
                                    .println("Key inserted at Bootstrap server: " + iKey + ": "
                                            + BootstrapServer.pairs.get(iKey));
                        }
                    } else {
                        String successorIP = BootstrapServer.successorNameServer.getNSIP();
                        int successorPort = BootstrapServer.successorNameServer.getNSPort();
                        try (Socket socket = new Socket(successorIP, successorPort)) {
                            BufferedReader readSucc = new BufferedReader(
                                    new InputStreamReader(socket.getInputStream()));
                            PrintWriter writeSucc = new PrintWriter(socket.getOutputStream(), true);
                            writeSucc.println("insert " + iKey + " " + iValue);
                            String[] succOutput = readSucc.readLine().split(" ");
                            if (succOutput[0].equals("null")) {
                                System.out.println("ERROR!! Invalid");
                            } else if (succOutput[0].equals("present")) {
                                System.out.println("Key already present in server > 0 > ");
                                for (int j = succOutput.length - 1; j > 1; j--) {
                                    System.out.print(succOutput[j] + " > ");
                                }
                                System.out.println(succOutput[1]);
                            } else {
                                System.out.print("Key-value inserted in server > 0 > ");
                                for (int j = 0; j < succOutput.length - 1; j++) {
                                    System.out.print(succOutput[j] + " > ");
                                }
                                System.out.println(succOutput[succOutput.length - 1]);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    continue;
                }

                else if ("delete".equalsIgnoreCase(arg[0])) {
                    int dKey = Integer.parseInt(arg[1]);
                    Set<Integer> i = BootstrapServer.pairs.keySet();
                    if (i.contains(dKey)) {
                        System.out.println("Key deleted from Bootstrap: 0 > " + BootstrapServer.pairs.get(dKey));
                        BootstrapServer.pairs.remove(dKey);
                    } else {
                        String successorIP = BootstrapServer.successorNameServer.getNSIP();
                        int successorPort = BootstrapServer.successorNameServer.getNSPort();
                        try (Socket socket = new Socket(successorIP, successorPort)) {
                            BufferedReader readSucc = new BufferedReader(
                                    new InputStreamReader(socket.getInputStream()));
                            PrintWriter writeSucc = new PrintWriter(socket.getOutputStream(), true);
                            writeSucc.println("delete " + dKey);
                            String[] succOutput = readSucc.readLine().split(" ");
                            if (succOutput[0].equals("null")) {
                                System.out.println("ERROR!! Not found");
                            } else {
                                System.out.println(succOutput.length);
                                for (String k : succOutput) {
                                    System.out.println(k);
                                }
                                System.out.print("Key deleted from server > 0 > ");
                                for (int j = 0; j <= succOutput.length - 2; j++) {
                                    System.out.print(succOutput[j] + " > ");
                                }
                                System.out.println(succOutput[succOutput.length - 1]);
                            }

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    continue;
                } else {
                    System.out.println("BootstrapServer> Invalid Command!! Try Again");
                    System.out.print("BootstrapServer> ");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class NameServer {
    private int nsID;
    private String nsIP;
    private int nsPort;

    public NameServer(int nsID, String nsIP, int nsPort) {
        this.nsID = nsID;
        this.nsIP = nsIP;
        this.nsPort = nsPort;
    }

    public int getNSID() {
        return nsID;
    }

    public String getNSIP() {
        return nsIP;
    }

    public int getNSPort() {
        return nsPort;
    }

    public void setNSID(int nsID) {
        this.nsID = nsID;
    }

    public void setNSIP(String nsIP) {
        this.nsIP = nsIP;
    }

    public void setNSPort(int nsPort) {
        this.nsPort = nsPort;
    }
}

class Node {
    int ID;
    Node next;
    Node previous;

    public Node(int ID) {
        this.ID = ID;
        this.next = null;
        this.previous = null;
    }
}

class DoubleLinkedList {
    private Node head;
    private Node tail;

    public DoubleLinkedList() {
        this.head = null;
        this.tail = null;
    }

    int getTail() {
        return tail.ID;
    }

    void add(int ID) {
        Node node = new Node(ID);

        if (head == null) {
            head = node;
            tail = node;
        } else if (ID <= head.ID) {
            node.next = head;
            head.previous = node;
            head = node;
        } else if (ID >= tail.ID) {
            tail.next = node;
            node.previous = tail;
            tail = node;
        } else {
            Node current = head;
            while (current.next != null && current.next.ID < ID) {
                current = current.next;
            }
            node.next = current.next;
            if (current.next != null) {
                current.next.previous = node;
            }
            current.next = node;
            node.previous = current;
        }
    }

    void delete(int ID) {
        if (head == null) {
            System.out.println("Linked List is empty.");
            return;
        }

        if (head.ID == ID) {
            head = head.next;
            if (head != null) {
                head.previous = null;
            } else {
                tail = null;
            }
            return;
        }

        Node current = head;
        while (current != null && current.ID != ID) {
            current = current.next;
        }

        if (current == null) {
            return;
        }

        if (current == tail) {
            tail = tail.previous;
            tail.next = null;
        } else {
            current.previous.next = current.next;
            current.next.previous = current.previous;
        }
    }

    Node getNode(int ID) {
        Node n = head;
        while (n != null) {
            if (n.ID == ID) {
                return n;
            }
            n = n.next;
        }
        return null;
    }
}