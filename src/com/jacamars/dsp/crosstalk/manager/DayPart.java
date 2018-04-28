package com.jacamars.dsp.crosstalk.manager;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

/**
 * Created by Ben M. Faul on 9/24/17.
 * Creates a dayparting structure. Each key in the set is (dayNumber * 24) + hourOfDay  is an active time and is placed
 * in the set "parts".
 * Form of constructor data is:
 *{
 *  "monday":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1],
 *  "tuesday":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
 *  "wednesday":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
 *  "thursday":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
 *  "friday":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
 *  "saturday":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
 *  "sunday":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
 *}
 *
 * If present day of week * 24 + hourOfDay is in the set in "parts" , then it is active, else it is not active.
 */
public class DayPart {
    /** Reader for the json used to build this */
    static ObjectMapper mapper = new ObjectMapper();
    static final List<String> DAYSOFWEEK = new ArrayList<>();
    static {
        DAYSOFWEEK.add("sunday");
        DAYSOFWEEK.add("monday");
        DAYSOFWEEK.add("tuesday");
        DAYSOFWEEK.add("wednesday");
        DAYSOFWEEK.add("thursday");
        DAYSOFWEEK.add("friday");
        DAYSOFWEEK.add("saturday");
    };

    /** The daypart set */
    Set<Integer> parts = new HashSet();

    /**
     * Create a daypart structure
     * @param data String. The JSON format as a string.
     * @throws Exception on JSON errors.
     */
    public DayPart(String data) throws Exception {
        Map m = mapper.readValue(data, Map.class);
        for (int i=0;i<DAYSOFWEEK.size();i++) {
            List<Integer> list = (List)m.get(DAYSOFWEEK.get(i));
            for (int time=0;time<list.size();time++) {
                int hour = list.get(time);
                if (hour == 1) {
                    parts.add((i*24)+time);
                }
            }
        }

    }

    /**
     * Is this daypart active at the present time?
     * @return boolean. Returns true if now time is active, else false
     */
    public boolean isActive() {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        //Calendar c = Calendar.getInstance();
        int dayOfWeek = c.get(Calendar.DAY_OF_WEEK) - 1;
        int thisHourOfDay = c.get(Calendar.HOUR_OF_DAY);

        int hour = 24 * dayOfWeek + thisHourOfDay;

        if (parts.contains(hour))
            return true;

        return false;
    }

    /**
     * Simple test program.
     * @param args String[]. Not used.
     * @throws Exception on JSON errors.
     */
    public static void main(String [] args) throws Exception {
        String stuff = "{" +
            "\"monday\":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]," +
            "\"tuesday\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]," +
            "\"wednesday\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]," +
            "\"thursday\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]," +
            "\"friday\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]," +
            "\"saturday\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]," +
            "\"sunday\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]" +
        "}";

        Calendar c = Calendar.getInstance();
        int dayOfWeek = c.get(Calendar.DAY_OF_WEEK) - 1;
        int thisHourOfDay = c.get(Calendar.HOUR_OF_DAY);

        System.out.println("DOW = " + dayOfWeek + ", HOD = " + thisHourOfDay);

        System.out.println(new DayPart(stuff).isActive());
    }
}
