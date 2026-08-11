package com.signomix.receiver.producer;

import com.signomix.common.tsdb.ApplicationDao;
import com.signomix.common.tsdb.IotDatabaseDao;
import com.signomix.common.tsdb.SignalDao;
import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;

@ApplicationScoped
public class DaoProducer {

    @Inject
    @DataSource("oltp")
    AgroalDataSource oltpDataSource;

    @Inject
    @DataSource("olap")
    AgroalDataSource olapDataSource;

    @Produces
    @ApplicationScoped
    @Named("dao")
    public IotDatabaseDao produceDao() {
        IotDatabaseDao dao = new IotDatabaseDao();
        dao.setDatasource(oltpDataSource);
        return dao;
    }

    @Produces
    @ApplicationScoped
    @Named("olapDao")
    public IotDatabaseDao produceOlapDao() {
        IotDatabaseDao dao = new IotDatabaseDao();
        dao.setDatasource(oltpDataSource);
        dao.setAnalyticDatasource(olapDataSource);
        return dao;
    }

    @Produces
    @ApplicationScoped
    public SignalDao produceSignalDao() {
        SignalDao signalDao = new SignalDao();
        signalDao.setDatasource(oltpDataSource);
        return signalDao;
    }

    @Produces
    @ApplicationScoped
    public ApplicationDao produceApplicationDao() {
        ApplicationDao appDao = new ApplicationDao();
        appDao.setDatasource(oltpDataSource);
        return appDao;
    }
}
