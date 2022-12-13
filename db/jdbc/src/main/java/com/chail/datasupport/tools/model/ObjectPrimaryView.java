package com.chail.datasupport.tools.model;

import lombok.Data;

@Data
public class ObjectPrimaryView {
    private String id;
    private String name;
    private EntityView entity;
}