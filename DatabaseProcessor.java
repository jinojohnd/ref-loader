package com.ml.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DatabaseProcessor {
  private static final String FILE_PATH = "D:/A/BK_Test.csv";
  private static final int BATCH_SIZE = 2;
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("MM-dd-yyyy");

  // In-memory database
  private List<Record> database = new ArrayList<>();

  // Record class to hold database row
  static class Record {
    Date startDate;
    Date endDate; // Nullable for infinite range
    boolean delete;
    String bk;
    int val;

    Record(String startDate, String endDate, boolean delete, String bk, int val) throws ParseException {
      this.startDate = DATE_FORMAT.parse(startDate);
      this.endDate = endDate.isEmpty() ? null : DATE_FORMAT.parse(endDate);
      this.delete = delete;
      this.bk = bk;
      this.val = val;
    }

    Record(Date startDate, Date endDate, boolean delete, String bk, int val) {
      this.startDate = startDate;
      this.endDate = endDate;
      this.delete = delete;
      this.bk = bk;
      this.val = val;
    }

    @Override
    public String toString() {
      return String.format("%s,%s,%b,%s,%d",
          DATE_FORMAT.format(startDate),
          endDate != null ? DATE_FORMAT.format(endDate) : "",
          delete,
          bk,
          val);
    }
  }

  // Display database contents
  private void displayDatabase() {
    System.out.println("\nCurrent Database Contents:");
    System.out.println("StartDate,EndDate,Delete,BK,Val");
    if (database.isEmpty()) {
      System.out.println("Database is empty");
    } else {
      // Sort by BK and then start date
      List<Record> sortedDatabase = new ArrayList<>(database);
      sortedDatabase.sort(Comparator.comparing((Record r) -> r.bk).thenComparing(r -> r.startDate));
      for (Record record : sortedDatabase) {
        System.out.println(record.toString());
      }
    }
  }

  // Check if two date ranges overlap
  private boolean isDateOverlap(Date start1, Date end1, Date start2, Date end2) {
    if (end1 == null && end2 == null) {
      return true; // Both infinite, overlap if either start date is on or before the other
    } else if (end1 == null) {
      return !start1.after(end2); // New record infinite, overlap if start1 <= end2
    } else if (end2 == null) {
      return !start2.after(end1); // Existing record infinite, overlap if start2 <= end1
    } else {
      return start1.compareTo(end2) <= 0 && start2.compareTo(end1) <= 0;
    }
  }

  // Unified method to handle exact matches and overlaps
  private boolean handleRecord(Record newRecord, List<Record> existingRecords) {
    List<Record> overlappingRecords = new ArrayList<>();

    // Single pass over existing records
    for (Record existing : existingRecords) {
      // Check for exact match or null endDate update (Scenario 1)
      if (newRecord.startDate.equals(existing.startDate) && newRecord.bk.equals(existing.bk)) {
        // Update if endDate is null or matches exactly
        if (newRecord.endDate == null || Objects.equals(newRecord.endDate, existing.endDate)) {
          existing.endDate = newRecord.endDate; // Update endDate
          existing.val = newRecord.val; // Update value
          return true; // Handled as exact match or null endDate update
        }
      }
      // Check for overlap (Scenario 2)
      if (isDateOverlap(newRecord.startDate, newRecord.endDate, existing.startDate, existing.endDate)) {
        overlappingRecords.add(existing);
      }
    }

    // Process overlaps if any (Scenario 2)
    if (!overlappingRecords.isEmpty()) {
      for (Record existing : overlappingRecords) {
        // Case 1: Existing record is fully contained within new record
        if (!existing.startDate.before(newRecord.startDate) &&
            (newRecord.endDate == null || // New record is infinite, contains all later records
                (existing.endDate != null && !existing.endDate.after(newRecord.endDate)))) {
          database.remove(existing); // Remove fully contained record
        }
        // Case 2: Existing record starts before new record
        else if (existing.startDate.before(newRecord.startDate)) {
          Calendar cal = Calendar.getInstance();
          cal.setTime(newRecord.startDate);
          cal.add(Calendar.DAY_OF_MONTH, -1);
          existing.endDate = cal.getTime(); // Adjust endDate
        }
        // Case 3: Existing record ends after new record
        else  {
          Calendar cal = Calendar.getInstance();
          cal.setTime(newRecord.endDate);
          cal.add(Calendar.DAY_OF_MONTH, 1);
          existing.startDate = cal.getTime(); // Adjust startDate
        }
      }
      // Add the new record
      database.add(new Record(newRecord.startDate, newRecord.endDate, newRecord.delete, newRecord.bk, newRecord.val));
      return true; // Handled as overlap
    }

    return false; // No match or overlap, add new record (Scenario 3)
  }

  // Process the CSV file
  private void processFile() {
    Map<String, List<Record>> bkMap = new HashMap<>();
    List<String> errors = new ArrayList<>();
    Set<String> invalidBks = new HashSet<>();

    try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
      String line;
      br.readLine(); // Skip header
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(",", -1); // Preserve empty fields
        if (parts.length != 5) {
          errors.add("Invalid row format: " + line);
          continue;
        }

        try {
          Record record = new Record(parts[0], parts[1], Boolean.parseBoolean(parts[2]), parts[3], Integer.parseInt(parts[4]));
          String currentBk = record.bk;

          // Skip if BK is already marked as invalid
          if (invalidBks.contains(currentBk)) {
            continue;
          }

          // Check if batch size is reached for a new BK
          if (bkMap.size() == BATCH_SIZE && !bkMap.containsKey(currentBk)) {
            System.out.println("Processing batch for BKs: " + bkMap.keySet());
            processBatch(bkMap, invalidBks);
            bkMap.clear();
          }

          // Add record to bkMap
          List<Record> bkRecords = bkMap.computeIfAbsent(currentBk, k -> new ArrayList<>());
          boolean hasOverlap = false;
          for (Record existing : bkRecords) {
            if (isDateOverlap(record.startDate, record.endDate, existing.startDate, existing.endDate)) {
              errors.add("Date overlap for BK " + currentBk + ": " + line);
              invalidBks.add(currentBk);
              hasOverlap = true;
              break;
            }
          }

          if (!hasOverlap) {
            bkRecords.add(record);
            // Sort by start date within BK
            bkRecords.sort(Comparator.comparing(r -> r.startDate));
          }

        } catch (ParseException | NumberFormatException e) {
          errors.add("Error processing row: " + line + " - " + e.getMessage());
        }
      }

      // Process any remaining records
      if (!bkMap.isEmpty()) {
        System.out.println("Processing final batch for BKs: " + bkMap.keySet());
        processBatch(bkMap, invalidBks);
      }

      // Display errors
      if (!errors.isEmpty()) {
        System.out.println("\nProcessing Errors:");
        errors.forEach(System.out::println);
      }

    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }

  // Process a batch of BKs
  private void processBatch(Map<String, List<Record>> bkMap, Set<String> invalidBks) {
    // Remove invalid BKs from the map
    invalidBks.forEach(bkMap::remove);

    for (Map.Entry<String, List<Record>> entry : bkMap.entrySet()) {
      String bk = entry.getKey();
      List<Record> newRecords = entry.getValue();

      // Get existing records for this BK from database, sorted by start date
      List<Record> existingRecords = database.stream()
          .filter(r -> r.bk.equals(bk))
          .sorted(Comparator.comparing(r -> r.startDate))
          .collect(Collectors.toList());

      // Process each new record
      for (Record newRecord : newRecords) {
        // Try unified handler
        if (!handleRecord(newRecord, existingRecords)) {
          // Scenario 3: No overlap or match, add new record
          database.add(newRecord);
        }
      }
    }

    // Sort entire database by BK and then start date
    database.sort(Comparator.comparing((Record r) -> r.bk).thenComparing(r -> r.startDate));
  }

  public static void main(String[] args) {
    DatabaseProcessor processor = new DatabaseProcessor();
    Scanner scanner = new Scanner(System.in);

    while (true) {
      // Display current database state
      processor.displayDatabase();

      // Prompt user
      System.out.print("\nLoad File? (Y/N): ");
      String input = scanner.nextLine().trim().toUpperCase();

      if (input.equals("Y")) {
        processor.processFile();
      } else if (input.equals("N")) {
        System.out.println("Exiting program.");
        break;
      } else {
        System.out.println("Invalid input. Please enter Y or N.");
      }
    }

    scanner.close();
  }
}