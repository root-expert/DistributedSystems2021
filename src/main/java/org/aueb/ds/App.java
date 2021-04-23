package org.aueb.ds;

public class App {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("You need to provide at least one argument!");
            return;
        }

        String what = args[0];
        switch (what) {
            case "broker":
                // start broker
                break;
            case "node":
                String ip = args[1];
                Integer port = Integer.parseInt(args[2]);
                // start Publisher, Consumer threads
                break;
            default:
                System.out.println("Invalid arguments");
                break;
        }
    }
}
