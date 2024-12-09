package com.indritvaka.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Data
@Builder
public class User implements Serializable {
    private String name;
    private String email;
    private String phone;
    private String address;
}
