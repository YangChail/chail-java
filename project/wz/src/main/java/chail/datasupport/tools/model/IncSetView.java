package chail.datasupport.tools.model;

import lombok.Data;

import java.util.List;
@Data
public class IncSetView {
    private List<ObjectPrimaryView> objectPrimary;
    private String insertMode;
    private ObjectIncView objectInc;
}