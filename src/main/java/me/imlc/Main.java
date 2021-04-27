package me.imlc;

public class Main {

  public static void main(String[] args) {
    startSimulacronServer();
    startCassandraClients(20);
  }

  public static void startSimulacronServer() {
    new Thread(() -> {
      Simulacron app = new Simulacron();
      app.start();
    }).start();
  }

  public static void startCassandraClients(int count) {
    for (int i = 0; i < count; i++) {
      new Thread(() -> {
        try {
          new CassandraClient().queryRepeatedly();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }).start();
    }
  }

}
