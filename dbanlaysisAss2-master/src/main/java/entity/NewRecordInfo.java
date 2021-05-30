package entity;

/**
 * new record struct
 * */
public class NewRecordInfo {
    public int id;
    public int Sensor_ID;
    public int Hourly_Counts;
    public String Date_Time;
    public int sensor_Name_len;
    public String Sensor_Name;


    public NewRecordInfo(String[] colArr) {
        try {
            this.id = Integer.parseInt(colArr[0]);
            this.Sensor_ID = Integer.parseInt(colArr[7]);
            this.Hourly_Counts = Integer.parseInt(colArr[9]);
            this.Date_Time = colArr[1];
            this.Sensor_Name = colArr[8];
            this.sensor_Name_len = this.Sensor_Name.length();
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public NewRecordInfo(int id, int sensor_ID, int hourly_Counts, String date_Time, int sensor_Name_len, String sensor_Name) {
        this.id = id;
        Sensor_ID = sensor_ID;
        Hourly_Counts = hourly_Counts;
        Date_Time = date_Time;
        this.sensor_Name_len = sensor_Name_len;
        Sensor_Name = sensor_Name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getSensor_ID() {
        return Sensor_ID;
    }

    public void setSensor_ID(int sensor_ID) {
        Sensor_ID = sensor_ID;
    }

    public int getHourly_Counts() {
        return Hourly_Counts;
    }

    public void setHourly_Counts(int hourly_Counts) {
        Hourly_Counts = hourly_Counts;
    }

    public String getSensor_Name() {
        return Sensor_Name;
    }

    public void setSensor_Name(String sensor_Name) {
        Sensor_Name = sensor_Name;
    }

    public String getDate_Time() {
        return Date_Time;
    }

    public void setDate_Time(String date_Time) {
        Date_Time = date_Time;
    }
}
