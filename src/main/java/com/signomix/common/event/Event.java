/*
 * Copyright 2016 Grzegorz Skorupa <g.skorupa at gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.signomix.common.event;

import java.time.Instant;

/**
 * Event
 */
public class Event {

    private long id = -1;
    private String name = null;
    private String category;
    private String type;
    private String origin;
    private Object payload;
    private long createdAt;

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * Creates new Event instance. Sets new id and createdAt parameters.
     */
    public Event() {
        createdAt=System.currentTimeMillis();
        id=createdAt;
    }

    /**
     * @return the id
     */
    public long getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type != null ? type : "";
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * @return the origin
     */
    public String getOrigin() {
        return origin;
    }

    /**
     * @param origin the origin to set
     */
    public void setOrigin(String origin) {
        this.origin = origin;
    }

    /**
     * @return the payload
     */
    public Object getPayload() {
        return payload;
    }

    /**
     * @param payload the payload to set
     */
    public void setPayload(Object payload) {
        this.payload = payload;
    }

    /**
     * @return the category
     */
    public String getCategory() {
        return category != null ? category : "";
    }

    /**
     * @param subtype the category to set
     */
    public void setCategory(String subtype) {
        this.category = subtype;
    }


    @Override
    public Event clone() {
        Event clon = new Event();
        clon.name = name;
        clon.category = category;
        clon.origin = origin;
        clon.payload = payload;
        clon.type = type;
        return clon;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
        //return this;
    }

    public Event putName(String name) {
        this.name = name;
        return this;
    }

}
