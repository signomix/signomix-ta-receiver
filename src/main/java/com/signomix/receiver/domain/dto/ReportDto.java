package com.signomix.receiver.domain.dto;

public class ReportDto {

    private double timestamp;
    private ServerDto server;
    private DockerContainerDto[] containers;
    private double dockerCheckStatus;
    private String dockerCheckMessage;
    private double systemCheckStatus;
    private String systemCheckMessage;

    public ReportDto(
        double timestamp,
        ServerDto server,
        DockerContainerDto[] containers
    ) {
        this(
            timestamp,
            server,
            containers,
            CheckStatus.OK,
            null,
            CheckStatus.OK,
            null
        );
    }

    public ReportDto(
        double timestamp,
        ServerDto server,
        DockerContainerDto[] containers,
        CheckStatus dockerCheckStatus,
        String dockerCheckMessage,
        CheckStatus systemCheckStatus,
        String systemCheckMessage
    ) {
        this.timestamp = timestamp;
        this.server = server;
        this.containers = containers;
        this.dockerCheckStatus = dockerCheckStatus.getValue();
        this.dockerCheckMessage = dockerCheckMessage;
        this.systemCheckStatus = systemCheckStatus.getValue();
        this.systemCheckMessage = systemCheckMessage;
    }

    public double getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(double timestamp) {
        this.timestamp = timestamp;
    }

    public ServerDto getServer() {
        return server;
    }

    public void setServer(ServerDto server) {
        this.server = server;
    }

    public DockerContainerDto[] getContainers() {
        return containers;
    }

    public void setContainers(DockerContainerDto[] containers) {
        this.containers = containers;
    }

    public double getDockerCheckStatus() {
        return dockerCheckStatus;
    }

    public void setDockerCheckStatus(double dockerCheckStatus) {
        this.dockerCheckStatus = dockerCheckStatus;
    }

    public String getDockerCheckMessage() {
        return dockerCheckMessage;
    }

    public void setDockerCheckMessage(String dockerCheckMessage) {
        this.dockerCheckMessage = dockerCheckMessage;
    }

    public double getSystemCheckStatus() {
        return systemCheckStatus;
    }

    public void setSystemCheckStatus(double systemCheckStatus) {
        this.systemCheckStatus = systemCheckStatus;
    }

    public String getSystemCheckMessage() {
        return systemCheckMessage;
    }

    public void setSystemCheckMessage(String systemCheckMessage) {
        this.systemCheckMessage = systemCheckMessage;
    }
}
