package it.flink.model;

/** Modello per l'output della Q2 */
public class OutlierOutput {
    public String batchId;
    public String printId;
    public String tileId;
    
    public String p1; public String dp1;
    public String p2; public String dp2;
    public String p3; public String dp3;
    public String p4; public String dp4;
    public String p5; public String dp5;
    
    public OutlierOutput(String batchId, String printId, String tileId,
                          String p1, String dp1, String p2, String dp2,
                          String p3, String dp3, String p4, String dp4,
                          String p5, String dp5) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.p1 = p1;
        this.dp1 = dp1;
        this.p2 = p2;
        this.dp2 = dp2;
        this.p3 = p3;
        this.dp3 = dp3;
        this.p4 = p4;
        this.dp4 = dp4;
        this.p5 = p5;
        this.dp5 = dp5;
    }

    public OutlierOutput() {}

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
            batchId, printId, tileId, p1, dp1, p2, dp2, p3, dp3, p4, dp4, p5, dp5);
    }
}