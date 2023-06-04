package com.reache.ddop.train.service.impl;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.reache.ddop.train.mapper.TrainStuduentMapper;
import com.reache.ddop.train.domain.TrainStuduent;
import com.reache.ddop.train.service.ITrainStuduentService;

/**
 * 学生信息Service业务层处理
 * 
 * @author shock
 * @date 2023-06-03
 */
@Service
public class TrainStuduentServiceImpl implements ITrainStuduentService 
{
    @Autowired
    private TrainStuduentMapper trainStuduentMapper;

    /**
     * 查询学生信息
     * 
     * @param studentId 学生信息主键
     * @return 学生信息
     */
    @Override
    public TrainStuduent selectTrainStuduentByStudentId(Long studentId)
    {
        return trainStuduentMapper.selectTrainStuduentByStudentId(studentId);
    }

    /**
     * 查询学生信息列表
     * 
     * @param trainStuduent 学生信息
     * @return 学生信息
     */
    @Override
    public List<TrainStuduent> selectTrainStuduentList(TrainStuduent trainStuduent)
    {
        return trainStuduentMapper.selectTrainStuduentList(trainStuduent);
    }

    /**
     * 新增学生信息
     * 
     * @param trainStuduent 学生信息
     * @return 结果
     */
    @Override
    public int insertTrainStuduent(TrainStuduent trainStuduent)
    {
        return trainStuduentMapper.insertTrainStuduent(trainStuduent);
    }

    /**
     * 修改学生信息
     * 
     * @param trainStuduent 学生信息
     * @return 结果
     */
    @Override
    public int updateTrainStuduent(TrainStuduent trainStuduent)
    {
        return trainStuduentMapper.updateTrainStuduent(trainStuduent);
    }

    /**
     * 批量删除学生信息
     * 
     * @param studentIds 需要删除的学生信息主键
     * @return 结果
     */
    @Override
    public int deleteTrainStuduentByStudentIds(Long[] studentIds)
    {
        return trainStuduentMapper.deleteTrainStuduentByStudentIds(studentIds);
    }

    /**
     * 删除学生信息信息
     * 
     * @param studentId 学生信息主键
     * @return 结果
     */
    @Override
    public int deleteTrainStuduentByStudentId(Long studentId)
    {
        return trainStuduentMapper.deleteTrainStuduentByStudentId(studentId);
    }
}
