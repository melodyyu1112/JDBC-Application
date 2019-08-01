import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;
import java.util.*;

/*
Runs queries against a back-end database.
This class is responsible for searching for flights.
*/

public class QuerySearchOnly {
  // `dbconn.properties` config file
  private String configFilename;
  protected List<Itinerary> itinerary_list;
  // DB Connection
  protected Connection conn;

  // Canned queries
  //private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  //protected PreparedStatement checkFlightCapacityStatement;

  private static final String SAFE_SEARCH_DIRECT = "SELECT TOP (?) f.fid, f.day_of_month, f.carrier_id, f.flight_num, f.origin_city, f.dest_city, f.actual_time, f.capacity, price "
  + "FROM Flights as f WHERE f.origin_city = ? AND f.dest_city = ? AND f.day_of_month = ? AND f.canceled = 0 ORDER BY actual_time ASC;";
  protected PreparedStatement safeSearchDirect;

  private static final String SAFE_SEARCH_ONE_HOP =
  "SELECT TOP (?) f1.fid as f1_fid, f2.fid as f2_fid, f1.day_of_month as f1_day_of_month, f2.day_of_month as f2_day_of_month, "
  + "f1.carrier_id as f1_carrier_id, f2.carrier_id as f2_carrier_id, "
  + "f1.flight_num as f1_flight_num, f1.origin_city as f1_origin_city, f1.dest_city as f1_dest_city, "
  + "f1.actual_time as f1_actual_time, f1.capacity as f1_capacity, f1.price as f1_price, f2.flight_num as f2_flight_num, "
  + "f2.origin_city as f2_origin_city, f2.dest_city as f2_dest_city, f2.actual_time as f2_actual_time, "
  + "f2.capacity as f2_capacity, f2.price as f2_price, f1.actual_time + f2.actual_time as actual_time "
  + "FROM Flights as f1, Flights as f2 "
  + "WHERE f1.origin_city = ? and f2.dest_city = ? and f1.dest_city = f2.origin_city and f1.day_of_month = ? and f2.day_of_month = ? "
  + "and f1.canceled = 0 and f2.canceled = 0 and f1.day_of_month = f2.day_of_month "
  + "ORDER BY actual_time ASC, f1.fid ASC, f2.fid ASC;";
  protected PreparedStatement safeSearchOneHop;


  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    public Flight(int fid, int dayOfMonth, String carrierId, String flightNum, String originCity, String destCity,
    int time, int capacity, int price) {
      this.fid = fid;
      this.dayOfMonth = dayOfMonth;
      this.carrierId = carrierId;
      this.flightNum = flightNum;
      this.originCity = originCity;
      this.destCity = destCity;
      this.time = time;
      this.capacity = capacity;
      this.price = price;
    }

    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
      " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
      " Capacity: " + capacity + " Price: " + price;
    }
  }

  class Itinerary implements Comparable<Itinerary> {

    public Flight f1;
    public Flight f2;
    public int price1;
    public int fid1;
    public int fid2;
    public int total_time;

    public Itinerary(int total_time, Flight f1, Flight f2, int cost) {
      this.total_time = total_time;
      this.f1 = f1;
      this.f2 = f2;
      this.price1 = cost;
    }

    //In all cases the returned results should be primarily sorted on actual_time.
    //If a tie occurs, break that tie by the fid value.
    //Use the first then second fid for tie breaking.
    //Return only flights that are not canceled, ignoring the capacity and number of seats available.
    @Override
    public int compareTo(Itinerary o) {
      if (this.total_time == o.total_time) {
        if (this.f1.fid == o.f1.fid) {
          if (this.f2 != null && o.f2 != null) {
            return this.f2.fid - o.f2.fid;
          }
        }
        return this.f1.fid - o.f1.fid;
      }
      return this.total_time - o.total_time;
    }
  }

  public QuerySearchOnly(String configFilename) {
    this.configFilename = configFilename;
  }

  /** Open a connection to SQL Server in Microsoft Azure.  */
  public void openConnection() throws Exception {
    Properties configProps = new Properties();
    configProps.load(new FileInputStream(configFilename));

    String jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    String jSQLUrl = configProps.getProperty("flightservice.url");
    String jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    String jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
    jSQLUser, // user
    jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement
    /* In the full Query class, you will also want to appropriately set the transaction's isolation level:
    conn.setTransactionIsolation(...)
    See Connection class's JavaDoc for details.
    */
  }

  public void closeConnection() throws Exception {
    conn.close();
  }

  /*
  * prepare all the SQL statements in this method.
  * "preparing" a statement is almost like compiling it.
  * Note that the parameters (with ?) are still not filled in
  */
  
  public void prepareStatements() throws Exception {
    //checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    safeSearchDirect = conn.prepareStatement(SAFE_SEARCH_DIRECT);
    safeSearchOneHop = conn.prepareStatement(SAFE_SEARCH_ONE_HOP);
    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */
  }

 /**
  * Implement the search function.
  *
  * Searches for flights from the given origin city to the given destination
  * city, on the given day of the month. If {@code directFlight} is true, it only
  * searches for direct flights, otherwise it searches for direct flights
  * and flights with two "hops." Only searches for up to the number of
  * itineraries given by {@code numberOfItineraries}.
  *
  * The results are sorted based on total flight time.
  *
  * @param originCity
  * @param destinationCity
  * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
  * @param dayOfMonth
  * @param numberOfItineraries number of itineraries to return
  *
  * @return If no itineraries were found, return "No flights match your selection\n".
  * If an error occurs, then return "Failed to search\n".
  *
  * Otherwise, the sorted itineraries printed in the following format:
  *
  * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
  * [first flight in itinerary]\n
  * ...
  * [last flight in itinerary]\n
  *
  * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
  * in each search should always start from 0 and increase by 1.
  *
  * @see Flight#toString()
  */
  
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
  int numberOfItineraries) {
    List<Itinerary> itineraries = new ArrayList<>();

    try {
      int itinerary_flight = 0;

        safeSearchDirect.clearParameters();
        safeSearchDirect.setInt(1, numberOfItineraries);
        safeSearchDirect.setString(2, originCity);
        safeSearchDirect.setString(3, destinationCity);
        safeSearchDirect.setInt(4, dayOfMonth);


        ResultSet directFlightResult = safeSearchDirect.executeQuery();
        while (directFlightResult.next()) {
          int resultFlightID = directFlightResult.getInt("fid");
          int resultDayOfMonth = directFlightResult.getInt("day_of_month");
          int resultTime = directFlightResult.getInt("actual_time");
          int resultCapacity = directFlightResult.getInt("capacity");
          int resultPrice = directFlightResult.getInt("price");
          String resultOriginCity = directFlightResult.getString("origin_city");
          String resultDestCity = directFlightResult.getString("dest_city");
          String resultFlightNum = directFlightResult.getString("flight_num");
          String resultCarrierId = directFlightResult.getString("carrier_id");

          Flight f = new Flight(resultFlightID, resultDayOfMonth, resultCarrierId, resultFlightNum, resultOriginCity, resultDestCity, resultTime, resultCapacity, resultPrice);

          itineraries.add(new Itinerary(resultTime, f, null, resultPrice));
          itinerary_flight++;
        }
        directFlightResult.close();
        if ((itinerary_flight < numberOfItineraries) && !directFlight) {
          safeSearchOneHop.setInt(1, numberOfItineraries - (itinerary_flight));
          safeSearchOneHop.setString(2, originCity);
          safeSearchOneHop.setString(3, destinationCity);
          safeSearchOneHop.setInt(4, dayOfMonth);
          safeSearchOneHop.setInt(5, dayOfMonth);

          ResultSet onehop = safeSearchOneHop.executeQuery();

          while (onehop.next()) {
            int resultFlightID = onehop.getInt("f1_fid");
            int resultDayOfMonth = onehop.getInt("f1_day_of_month");
            String resultCarrierId = onehop.getString("f1_carrier_id");
            String resultFlightNum = onehop.getString("f1_flight_num");
            String resultOriginCity = onehop.getString("f1_origin_city");
            String resultDestCity = onehop.getString("f1_dest_city");
            int resultTime = onehop.getInt("f1_actual_time");
            int resultCapacity = onehop.getInt("f1_capacity");
            int resultPrice = onehop.getInt("f1_price");
            int resultFlightID2 = onehop.getInt("f2_fid");
            int resultDayOfMonth2 = onehop.getInt("f2_day_of_month");
            String resultCarrierId2 = onehop.getString("f2_carrier_id");
            String resultFlightNum2 = onehop.getString("f2_flight_num");
            String resultOriginCity2 = onehop.getString("f2_origin_city");
            String resultDestCity2 = onehop.getString("f2_dest_city");
            int resultTime2 = onehop.getInt("f2_actual_time");
            int resultCapacity2 = onehop.getInt("f2_capacity");
            int resultPrice2 = onehop.getInt("f2_price");

            Flight f1 = new Flight(resultFlightID, resultDayOfMonth, resultCarrierId, resultFlightNum, resultOriginCity, resultDestCity, resultTime, resultCapacity, resultPrice);
            Flight f2 = new Flight(resultFlightID2, resultDayOfMonth2, resultCarrierId2, resultFlightNum2, resultOriginCity2, resultDestCity2, resultTime2, resultCapacity2, resultPrice2);
            itineraries.add(new Itinerary((resultTime + resultTime2), f1, f2, (resultPrice + resultPrice2)));
          }
        onehop.close();
      }

    String search = printItineraries(itineraries);
        if (search.equals("")) {
            return "No flights match your selection\n";
        }
        return search;
    } catch (SQLException e) {
      e.printStackTrace();
      return "Failed";
    }
  }
  
  // Implement my own safe version that uses prepared statements rather than string concatenation.
  //return transaction_search_unsafe(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);

  private String printItineraries(List<Itinerary> itineraries) {
    Collections.sort(itineraries);
    StringBuffer sb = new StringBuffer();
    Itinerary itin = null;
    for (int i = 0; i < itineraries.size(); i++) {
      itin = itineraries.get(i);
      int n = 1;
      if (itin.f2 != null){
        n = 2;
      }

      sb.append("Itinerary " + i + ": " + n + " flight(s), " + itin.total_time + " minutes\n");
      sb.append(itin.f1.toString() + "\n");
        if (itin.f2 != null) {
            sb.append(itin.f2.toString() + "\n");
        }
      }
    return sb.toString();
  }
