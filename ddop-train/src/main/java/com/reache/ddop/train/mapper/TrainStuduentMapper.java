package com.reache.ddop.train.mapper;

import java.util.List;
import com.reache.ddop.train.domain.TrainStuduent;

/**
 * 学生信息Mapper接口
 * 
 * @author shock
 * @date 2023-06-03
 */
public interface TrainStuduentMapper 
{
    /**
     * 查询学生信息
     * 
     * @param studentId 学生信息主键
     * @return 学生信息
     */
    public TrainStuduent selectTrainStuduentByStudentId(Long studentId);

    /**
     * 查询学生信息列表
     * 
     * @param trainStuduent 学生信息
     * @return 学生信息集合
     */
    public List<TrainStuduent> selectTrainStuduentList(TrainStuduent trainStuduent);

    /**
     * 新增学生信息
     * 
     * @param trainStuduent 学生信息
     * @return 结果
     */
    public int insertTrainStuduent(TrainStuduent trainStuduent);

    /**
     * 修改学生信息
     * 
     * @param trainStuduent 学生信息
     * @return 结果
     */
    public int updateTrainStuduent(TrainStuduent trainStuduent);

    /**
     * 删除学生信息
     * 
     * @param studentId 学生信息主键
     * @return 结果
     */
    public int deleteTrainStuduentByStudentId(Long studentId);

    /**
     * 批量删除学生信息
     * 
     * @param studentIds 需要删除的数据主键集合
     * @return 结果
     */
    public int deleteTrainStuduentByStudentIds(Long[] studentIds);
}
