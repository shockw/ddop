package com.reache.ddop.train.controller;

import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.reache.ddop.common.annotation.Log;
import com.reache.ddop.common.core.controller.BaseController;
import com.reache.ddop.common.core.domain.AjaxResult;
import com.reache.ddop.common.enums.BusinessType;
import com.reache.ddop.train.domain.TrainStuduent;
import com.reache.ddop.train.service.ITrainStuduentService;
import com.reache.ddop.common.utils.poi.ExcelUtil;
import com.reache.ddop.common.core.page.TableDataInfo;

/**
 * 学生信息Controller
 * 
 * @author shock
 * @date 2023-06-03
 */
@RestController
@RequestMapping("/train/studuent")
public class TrainStuduentController extends BaseController
{
    @Autowired
    private ITrainStuduentService trainStuduentService;

    /**
     * 查询学生信息列表
     */
    @PreAuthorize("@ss.hasPermi('train:studuent:list')")
    @GetMapping("/list")
    public TableDataInfo list(TrainStuduent trainStuduent)
    {
        startPage();
        List<TrainStuduent> list = trainStuduentService.selectTrainStuduentList(trainStuduent);
        return getDataTable(list);
    }

    /**
     * 导出学生信息列表
     */
    @PreAuthorize("@ss.hasPermi('train:studuent:export')")
    @Log(title = "学生信息", businessType = BusinessType.EXPORT)
    @PostMapping("/export")
    public void export(HttpServletResponse response, TrainStuduent trainStuduent)
    {
        List<TrainStuduent> list = trainStuduentService.selectTrainStuduentList(trainStuduent);
        ExcelUtil<TrainStuduent> util = new ExcelUtil<TrainStuduent>(TrainStuduent.class);
        util.exportExcel(response, list, "学生信息数据");
    }

    /**
     * 获取学生信息详细信息
     */
    @PreAuthorize("@ss.hasPermi('train:studuent:query')")
    @GetMapping(value = "/{studentId}")
    public AjaxResult getInfo(@PathVariable("studentId") Long studentId)
    {
        return success(trainStuduentService.selectTrainStuduentByStudentId(studentId));
    }

    /**
     * 新增学生信息
     */
    @PreAuthorize("@ss.hasPermi('train:studuent:add')")
    @Log(title = "学生信息", businessType = BusinessType.INSERT)
    @PostMapping
    public AjaxResult add(@RequestBody TrainStuduent trainStuduent)
    {
        return toAjax(trainStuduentService.insertTrainStuduent(trainStuduent));
    }

    /**
     * 修改学生信息
     */
    @PreAuthorize("@ss.hasPermi('train:studuent:edit')")
    @Log(title = "学生信息", businessType = BusinessType.UPDATE)
    @PutMapping
    public AjaxResult edit(@RequestBody TrainStuduent trainStuduent)
    {
        return toAjax(trainStuduentService.updateTrainStuduent(trainStuduent));
    }

    /**
     * 删除学生信息
     */
    @PreAuthorize("@ss.hasPermi('train:studuent:remove')")
    @Log(title = "学生信息", businessType = BusinessType.DELETE)
	@DeleteMapping("/{studentIds}")
    public AjaxResult remove(@PathVariable Long[] studentIds)
    {
        return toAjax(trainStuduentService.deleteTrainStuduentByStudentIds(studentIds));
    }
}
