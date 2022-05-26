public class Task {

    private Integer delay;
    private String type;

    public Integer getDelay() {
        return delay;
    }

    public void setDelay(Integer delay) {
        this.delay = delay;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Task(String type, Integer delay) {
        this.type = type;
        this.delay = delay;
    }
    public Task(String type) {
        this.type = type;
        this.delay = 0;
    }

}