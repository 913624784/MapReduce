package Work2;

import java.util.ArrayList;
import java.util.List;

public class Weibo {
    private int id;
    private String postid="";
    private int score;
    private String text="";
    private String creationdate="";
    private String userid="";

    public static Weibo util(String line) {
        String str[] = line.trim().split("\"");
        List<String> list = new ArrayList();
        int i = 1;
        int k = 0;
        while (i < str.length) {
            list.add(str[i]);
            i = i + 2;
        }
        Weibo weibo = new Weibo();
        if (str.length >= 11 && str[str.length - 3].equals(" UserId=")) {
            if (str[4].equals(" Text=")) {//没有score
                weibo.setId(Integer.parseInt(list.get(0)));
                weibo.setPostid(list.get(1));
                weibo.setText(list.get(2));
                weibo.setCreationdate(list.get(3));
                weibo.setUserid(list.get(4));
            } else {//有score
                weibo.setId(Integer.parseInt(list.get(0)));
                weibo.setPostid(list.get(1));
                weibo.setScore(Integer.parseInt(list.get(2)));
                weibo.setText(list.get(3));
                weibo.setCreationdate(list.get(4));
                weibo.setUserid(list.get(5));
            }
        }
        return weibo;
    }

    @Override
    public String toString() {
        return "Weibo{" +
                "id=" + id +
                ", postid=" + postid +
                ", score=" + score +
                ", text='" + text + '\'' +
                ", creationdate='" + creationdate + '\'' +
                ", userid=" + userid +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPostid() {
        return postid;
    }

    public void setPostid(String postid) {
        this.postid = postid;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getCreationdate() {
        return creationdate;
    }

    public void setCreationdate(String creationdate) {
        this.creationdate = creationdate;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }



}
