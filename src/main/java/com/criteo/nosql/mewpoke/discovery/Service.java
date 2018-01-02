package com.criteo.nosql.mewpoke.discovery;

public class Service {
    protected final String clusterName;
    protected final String bucketName;


    public Service(String clusterName, String bucketName) {
        this.clusterName = clusterName;
        this.bucketName = bucketName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBucketName() {
        return bucketName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Service service = (Service) o;

        if (clusterName != null ? !clusterName.equals(service.clusterName) : service.clusterName != null) return false;
        return bucketName != null ? bucketName.equals(service.bucketName) : service.bucketName == null;
    }

    @Override
    public int hashCode() {
        int result = clusterName != null ? clusterName.hashCode() : 0;
        result = 31 * result + (bucketName != null ? bucketName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Service {cluster='")
                .append(clusterName)
                .append("', bucket='")
                .append(bucketName)
                .append("'}")
                .toString();

    }
}
