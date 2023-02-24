package myflink;

public class Myrow {
    public String m_name,m_value,m_timestamp;

    public Myrow() {}

    public Myrow(String m_name, String m_value, String m_timestamp) {
        this.m_name = m_name;
        this.m_value = m_value;
        this.m_timestamp = m_timestamp;
    }

    @Override
    public String toString() {
        return "Myrow{" +
                "m_name='" + m_name + '\'' +
                ", m_value='" + m_value + '\'' +
                ", m_timestamp='" + m_timestamp + '\'' +
                '}';
    }
}
