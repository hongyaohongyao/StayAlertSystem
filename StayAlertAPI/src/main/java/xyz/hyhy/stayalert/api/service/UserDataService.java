package xyz.hyhy.stayalert.api.service;

import org.springframework.stereotype.Service;
import xyz.hyhy.stayalert.api.dao.UserDataDAO;
import xyz.hyhy.stayalert.api.entity.UserData;

import javax.annotation.Resource;

@Service("userDataService")
public class UserDataService {
    @Resource
    private UserDataDAO userDataDAO;

    public UserData getUserData(String userId) {
        return userDataDAO.getUserData(userId);
    }
}
