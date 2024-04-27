package nameServer;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class NServer {
    static Map<Integer, String> pairs = new TreeMap<>();
    static NameServer pred, succ;
    final static boolean[] thread = { false };

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("ERROR!! Please enter config file");
            return;
        }

        final String config = args[0];

        try {
            final String fileContents[] = new String[3];

            BufferedReader reader = new BufferedReader(new FileReader(config));
            for (int i = 0; i < 3; i++) {
                fileContents[i] = reader.readLine();
            }

            reader.close();

            int nameServerID = Integer.parseInt(fileContents[0]);
            int nameServerPort = Integer.parseInt(fileContents[1]);

            String bootstrapIP = fileContents[2].split(" ")[0];
            int bootstrapPort = Integer.parseInt(fileContents[2].split(" ")[1]);

            BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in));

            while (true) {
                System.out.print("NameServer> ");

                // Reading input from user
                String readLine = userInput.readLine();

                String[] arg = readLine.split(" ");

                if ("enter".equalsIgnoreCase(arg[0])) {
                    try (Socket socket = new Socket(bootstrapIP, bootstrapPort);
                            BufferedReader read1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            PrintWriter write1 = new PrintWriter(socket.getOutputStream(), true);) {
                        System.out.println("Successful Entry!!");
                        write1.println("enter " + nameServerID + " " + nameServerPort);

                        String[] bsline = read1.readLine().split(" ");
                        // for (String i : bsline)
                        // System.out.println(i);
                        int predID = Integer.parseInt(bsline[0]);
                        String predIP = bsline[1];
                        int predPort = Integer.parseInt(bsline[2]);
                        pred = new NameServer(predID, predIP, predPort);
                        System.out.println("Predecessor ID: " + pred.getNSID());
                        int succID = Integer.parseInt(bsline[3]);
                        String succIP = bsline[4];
                        int succPort = Integer.parseInt(bsline[5]);
                        succ = new NameServer(succID, succIP, succPort);
                        System.out.println("Successor ID " + succ.getNSID());
                        read1.close();
                        write1.close();
                        socket.close();
                    }
                    try (Socket predSocket = new Socket(NServer.pred.getNSIP(), NServer.pred.getNSPort());
                            PrintWriter predWrite = new PrintWriter(predSocket.getOutputStream(), true);) {
                        predWrite.println("enterPredUpdate " + nameServerID + " " + nameServerPort);
                        predWrite.close();
                        predSocket.close();
                    }
                    try (Socket succSocket = new Socket(NServer.succ.getNSIP(), NServer.succ.getNSPort());
                            PrintWriter succWrite = new PrintWriter(succSocket.getOutputStream(), true);) {
                        succWrite.println("enterSuccUpdate " + nameServerID + " " + nameServerPort);
                        succWrite.close();
                        succSocket.close();
                    }
                    try (Socket a = new Socket(succ.getNSIP(), succ.getNSPort());
                            BufferedReader read2 = new BufferedReader(new InputStreamReader(a.getInputStream()));
                            PrintWriter write2 = new PrintWriter(a.getOutputStream(), true)) {
                        write2.println("acquire " + nameServerID);
                        String bsLine;
                        System.out.println("Keys that will be managed by this server:");
                        while ((bsLine = read2.readLine()) != null) {
                            String[] b = bsLine.split(" ");
                            NServer.pairs.put(Integer.parseInt(b[0]), b[1]);
                            System.out
                                    .println(
                                            Integer.parseInt(b[0]) + " : " + NServer.pairs.get(Integer.parseInt(b[0])));
                        }

                        read2.close();
                        write2.close();
                        a.close();
                    }

                    NServer.thread[0] = true;
                    new Thread(new UserInteractionThread(nameServerID, nameServerPort)).start();
                    continue;
                }

                else if ("exit".equalsIgnoreCase(readLine)) {
                    NServer.thread[0] = false;
                    try (Socket predSocket = new Socket(NServer.pred.getNSIP(), NServer.pred.getNSPort());
                            PrintWriter predWrite = new PrintWriter(predSocket.getOutputStream(), true);) {
                        predWrite.println("exitPredUpdate " + NServer.succ.getNSID() + " " + NServer.succ.getNSPort());
                        predWrite.close();
                        predSocket.close();
                    }
                    try (Socket succSocket = new Socket(NServer.succ.getNSIP(), NServer.succ.getNSPort());
                            PrintWriter succWrite = new PrintWriter(succSocket.getOutputStream(), true);) {
                        succWrite.println("exitSuccUpdate " + NServer.pred.getNSID() + " " + NServer.pred.getNSPort());
                        succWrite.close();
                        succSocket.close();
                    }
                    try (Socket a = new Socket(succ.getNSIP(), succ.getNSPort());
                            BufferedReader read2 = new BufferedReader(new InputStreamReader(a.getInputStream()));
                            PrintWriter write2 = new PrintWriter(a.getOutputStream(), true)) {
                        write2.println("handover " + nameServerID);
                        List<Integer> temp = new ArrayList<>();

                        System.out.println("Successful Exit");
                        System.out.println("Key range that was handed over: ");
                        for (var i : NServer.pairs.entrySet()) {
                            System.out.println(i.getKey() + " : " + i.getValue());
                            write2.println(i.getKey() + " " + i.getValue());
                            temp.add(i.getKey());
                        }
                        for (int i : temp) {
                            NServer.pairs.remove(i);
                        }
                        read2.close();
                        write2.close();
                        a.close();
                    }
                    try (Socket socket = new Socket(bootstrapIP, bootstrapPort);
                            BufferedReader read1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            PrintWriter write1 = new PrintWriter(socket.getOutputStream(), true);) {
                        write1.println("exit " + nameServerID + " " + nameServerPort + " " + succ.getNSID() + " "
                                + succ.getNSIP() + " " + succ.getNSPort());
                        read1.close();
                        write1.close();
                        socket.close();
                    }
                    continue;
                } else {
                    System.out.println("Nameserver> Invalid Command!! Try Again");
                    System.out.print("NameServer> ");
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

class UserInteractionThread extends Thread {
    private static int nameServerID;
    private int nameServerPort;

    public UserInteractionThread(int nameServerID, int nameServerPort) {
        UserInteractionThread.nameServerID = nameServerID;
        this.nameServerPort = nameServerPort;
    }

    @Override
    public void run() {
        try (ServerSocket uit = new ServerSocket(nameServerPort)) {
            while (NServer.thread[0]) {
                Socket cSocket = uit.accept();
                BufferedReader read = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
                PrintWriter write = new PrintWriter(cSocket.getOutputStream(), true);
                String[] nsLine = read.readLine().split(" ");

                if (nsLine[0].equalsIgnoreCase("acquire")) {
                    System.out.println(acquire(read, write, nsLine));
                }
                if (nsLine[0].equalsIgnoreCase("handover")) {
                    System.out.println(handover(read, write, nsLine));
                } else if (nsLine[0].equalsIgnoreCase("enterPredUpdate")) {
                    if (NServer.succ.getNSID() == 0 || NServer.succ.getNSID() > Integer.parseInt(nsLine[1])) {
                        NServer.succ.setNSID(Integer.parseInt(nsLine[1]));
                        NServer.succ.setNSIP(cSocket.getInetAddress().getHostAddress());
                        NServer.succ.setNSPort(Integer.parseInt(nsLine[2]));
                    }
                } else if (nsLine[0].equalsIgnoreCase("enterSuccUpdate")) {
                    if (NServer.pred.getNSID() < Integer.parseInt(nsLine[1])) {
                        NServer.pred.setNSID(Integer.parseInt(nsLine[1]));
                        NServer.pred.setNSIP(cSocket.getInetAddress().getHostAddress());
                        NServer.pred.setNSPort(Integer.parseInt(nsLine[2]));
                    }
                } else if (nsLine[0].equalsIgnoreCase("exitPredUpdate")) {
                    if (NServer.succ.getNSID() == 0 || NServer.succ.getNSID() < Integer.parseInt(nsLine[1])) {
                        NServer.succ.setNSID(Integer.parseInt(nsLine[1]));
                        NServer.succ.setNSIP(cSocket.getInetAddress().getHostAddress());
                        NServer.succ.setNSPort(Integer.parseInt(nsLine[2]));
                    }
                } else if (nsLine[0].equalsIgnoreCase("exitSuccUpdate")) {
                    if (NServer.pred.getNSID() > Integer.parseInt(nsLine[1])) {
                        NServer.pred.setNSID(Integer.parseInt(nsLine[1]));
                        NServer.pred.setNSIP(cSocket.getInetAddress().getHostAddress());
                        NServer.pred.setNSPort(Integer.parseInt(nsLine[2]));
                    }
                } else if (nsLine[0].equalsIgnoreCase("lookup")) {
                    write.println(lookup(read, write, nsLine));
                } else if (nsLine[0].equalsIgnoreCase("insert")) {
                    write.println(insert(read, write, nsLine));
                } else if (nsLine[0].equalsIgnoreCase("delete")) {
                    write.println(delete(read, write, nsLine));
                }
                read.close();
                write.close();
                cSocket.close();
            }
            System.out.println("Terminated");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static String acquire(BufferedReader read, PrintWriter write, String[] nsLine) {
        int nsID = Integer.parseInt(nsLine[1]);

        List<Integer> temp = new ArrayList<>();
        for (var i : NServer.pairs.entrySet()) {
            int key = i.getKey();
            if (key <= nsID) {
                write.println(key + " " + i.getValue());
                temp.add(key);
            }
        }
        for (int i : temp) {
            NServer.pairs.remove(i);
        }
        for (Entry<Integer, String> i : NServer.pairs.entrySet()) {
            System.out.println(i.getKey() + " : " + i.getValue());
        }
        return "Key-value pairs shared to server : " + nsID;
    }

    public static String handover(BufferedReader read, PrintWriter write, String[] nsLine)
            throws NumberFormatException, IOException {
        System.out.println("keys that will be managed by this server: ");
        int nsID = Integer.parseInt(nsLine[1]);
        String bsLine;
        while ((bsLine = read.readLine()) != null) {
            String[] b = bsLine.split(" ");
            NServer.pairs.put(Integer.parseInt(b[0]), b[1]);
            System.out.println(Integer.parseInt(b[0]) + " : " + NServer.pairs.get(Integer.parseInt(b[0])));
        }
        return ("Key-value pairs received from server : " + nsID);
    }

    public static String lookup(BufferedReader read, PrintWriter write, String[] nsLine) {
        int lKey = Integer.parseInt(nsLine[1]);
        Set<Integer> i = NServer.pairs.keySet();
        if (i.contains(lKey)) {
            return (nameServerID + " " + NServer.pairs.get(lKey));
        } else if (NServer.succ.getNSID() == 0) {
            return "null";
        } else {
            String succIP = NServer.succ.getNSIP();
            int succPort = NServer.succ.getNSPort();
            try (Socket socket = new Socket(succIP, succPort)) {
                BufferedReader readSucc = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writeSucc = new PrintWriter(socket.getOutputStream(), true);
                writeSucc.println("lookup " + lKey);
                String succOutput = readSucc.readLine();
                if (succOutput.equals("null")) {
                    return ("null");
                } else {
                    return (nameServerID + " " + succOutput);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "null";
    }

    public static String insert(BufferedReader read, PrintWriter write, String[] nsLine) {
        int iKey = Integer.parseInt(nsLine[1]);
        String iValue = nsLine[2];
        if (iKey > NServer.pred.getNSID() && iKey < nameServerID) {
            if (NServer.pairs.containsKey(iKey)) {
                write.println("present " + nameServerID);
            } else {
                NServer.pairs.put(iKey, iValue);
                write.println(String.valueOf(nameServerID));
            }
        } else if (NServer.succ.getNSID() == 0) {
            return "null";
        } else {
            String succIP = NServer.succ.getNSIP();
            int succPort = NServer.succ.getNSPort();
            try (Socket socket = new Socket(succIP, succPort)) {
                BufferedReader readSucc = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writeSucc = new PrintWriter(socket.getOutputStream(), true);
                writeSucc.println("insert " + iKey + " " + iValue);
                String succOutput = readSucc.readLine();
                if (succOutput.equals("null")) {
                    return ("null");
                } else if (succOutput.split(" ")[0].equals("present")) {
                    return (succOutput + " " + nameServerID);
                } else {
                    return (nameServerID + " " + succOutput);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "null";
    }

    public static String delete(BufferedReader read, PrintWriter write, String[] nsLine) {
        int dKey = Integer.parseInt(nsLine[1]);
        Set<Integer> i = NServer.pairs.keySet();
        if (i.contains(dKey) && dKey != nameServerID) {
            NServer.pairs.remove(dKey);
            return (String.valueOf(nameServerID));
        } else if (NServer.succ.getNSID() == 0) {
            return "null";
        } else {
            String succIP = NServer.succ.getNSIP();
            int succPort = NServer.succ.getNSPort();
            try (Socket socket = new Socket(succIP, succPort)) {
                BufferedReader readSucc = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writeSucc = new PrintWriter(socket.getOutputStream(), true);
                writeSucc.println("delete " + dKey);
                String succOutput = readSucc.readLine();
                if (succOutput.equals("null")) {
                    return ("null");
                } else {
                    return (nameServerID + " " + succOutput);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "null";
    }
}
