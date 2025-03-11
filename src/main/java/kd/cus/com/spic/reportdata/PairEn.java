package kd.cus.com.spic.reportdata;

public class PairEn {
    private Boolean suc;
    private String msg;

    public PairEn(Boolean suc,String msg){
        this.suc = suc;
        this.msg = msg;
    }

    public Boolean getSuc() {
        return this.suc;
    }

    public void setSuc(final Boolean suc) {
        this.suc = suc;
    }

    public String getMsg() {
        return this.msg;
    }

    public void setMsg(final String msg) {
        this.msg = msg;
    }
}
