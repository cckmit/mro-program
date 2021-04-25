package mro.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author : lijichen
 * @date :  2020/5/12  16:24
 * @description: SFTP实体
 */
@Data
@AllArgsConstructor
public class SftpConnect {
    String ip;
    int port;
    String userName;
    String password;
}
