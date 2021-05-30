package index;

public class RecordEntity {
    public int id;
    public int year;
    public int month;
    public int day;
    public int hour;
    public int minute;
    public int second;
    public int weekDay;
    public int sensorID;
    public String sensorName;
    public int hourlyCount;

    public RecordEntity(int id, int year, int month, int day, int hour, int minute, int second, int weekDay, int sensorID, String sensorName, int hourlyCount) {
        this.id = id;
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.weekDay = weekDay;
        this.sensorID = sensorID;
        this.sensorName = sensorName;
        this.hourlyCount = hourlyCount;
    }

    public String dateTimeNumToStr() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%02d", this.month)).append('/');
        sb.append(String.format("%02d", this.day)).append('/');
        sb.append(String.format("%02d", this.year)).append(' ');
        boolean isPM = this.hour > 12;
        if (isPM) {
            sb.append(String.format("%02d", this.hour - 12));
        } else {
            sb.append(String.format("%02d", this.hour));
        }
        sb.append(':').append(String.format("%02d", this.minute));
        sb.append(':').append(String.format("%02d", this.second));
        sb.append(' ').append(isPM ? "PM" : "AM");
        return sb.toString();
    }

    public boolean isMatchBySearchText(String text) {
        String matchStr = sensorID +"_"+ dateTimeNumToStr();
//            System.out.println(matchStr);
        return matchStr.equals(text);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(',');
        sb.append(dateTimeNumToStr()).append(',');
        sb.append(this.year).append(',');
        sb.append(monthNumToStr()).append(',');
        sb.append(this.day).append(',');
        sb.append(weekNumToStr()).append(',');
        sb.append(this.hour).append(',');
        sb.append(this.sensorID).append(',');
        sb.append(this.sensorName).append(',');
        sb.append(this.hourlyCount);
        return sb.toString();
    }

    public String monthNumToStr() {
        switch (this.month) {
            case 1:
                return "January";
            case 2:
                return "February";
            case 3:
                return "March";
            case 4:
                return "April";
            case 5:
                return "May";
            case 6:
                return "June";
            case 7:
                return "July";
            case 8:
                return "August";
            case 9:
                return "September";
            case 10:
                return "October";
            case 11:
                return "November";
            case 12:
                return "December";
        }
        return "no month";
    }

    public String weekNumToStr() {
        switch (this.weekDay) {
            case 1:
                return "Monday";
            case 2:
                return "Tuesday";
            case 3:
                return "Wednesday";
            case 4:
                return "Thursday";
            case 5:
                return "Friday";
            case 6:
                return "Saturday";
            case 7:
                return "Sunday";
        }
        return "no week";
    }
}
