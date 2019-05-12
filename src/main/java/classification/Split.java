package classification;

import org.apache.hadoop.fs.FileUtil;
import org.datavec.api.io.filters.BalancedPathFilter;
import org.datavec.api.io.labels.ParentPathLabelGenerator;
import org.datavec.api.split.FileSplit;
import org.datavec.api.split.InputSplit;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Random;

public class Split {

    private static final String [] allowedExtensions = new String[]{"txt"};

    private static final long seed = 12345;

    private static final Random randNumGen = new Random(seed);

    private static final String dataDirFull = "D:/food";

    private static final String allDir = "all";

    private static final String trainDir = "train";

    private static final String valDir = "val";

    private static final String allDirFull = dataDirFull + "/" + allDir + "/";

    private static final String trainDirFull = dataDirFull + "/" + trainDir + "/";

    private static final String valDirFull = dataDirFull + "/" + valDir + "/";

    private static final String fileString = "file:///";

    private static final String hadoopDir = "C:\\hadoop\\";

    /**
     * Create the validation and train file sets.
     *
     * @param data
     * @param dirFull
     * @throws Exception
     */
    protected static void createFileSet(InputSplit data, String dirFull) throws Exception {
        /* copy all files to train folder */
        Integer counter = 0;
        for (Iterator<URI> locationsIterator = data.locationsIterator(); locationsIterator.hasNext();) {
            counter++;
            URI sourceFileURI = locationsIterator.next();

            File sourceFile = new File(sourceFileURI.toString().replace(fileString, ""));
            File targetFile = new File(sourceFile.getPath().replace(
                    allDirFull.replace("/", "\\"),
                    dirFull.replace("/", "\\")
            ));
            File targetFolder = new File(targetFile.getParentFile().toString());

            /* The folder path exists but is not a folder! */
            if (targetFolder.exists() && !targetFolder.isDirectory()) {
                throw new Exception(targetFolder.getPath() + " is not a folder! Error!");
            }

            /* Create folder if it does not exist. */
            if (!targetFolder.exists()) {
                System.out.println("Create missing folder " + targetFolder.getPath());
                targetFolder.mkdirs();
            }

            /* Create symlink */
            createSymlink(sourceFile, targetFile);
        }
    }

    protected static void deleteFolder(String path) {
        File folder = new File(path);

        if (folder.exists()) {
            System.out.println("Delete folder " + folder.getPath());
            folder.delete();
        }

        if (!folder.exists()) {
            System.out.println("Create empty folder " + folder.getPath());
            folder.mkdirs();
        }
    }

    protected static void createSymlink(File sourceFile, File targetFile) throws IOException {
        /* Delete existing symlink / file */
        if (targetFile.exists()) {
            targetFile.delete();
        }

        /* Create symlink */
        System.out.println(System.getProperty("hadoop.home.dir"));
        System.out.println("Create symlink " + targetFile.getPath());
        FileUtil.symLink(sourceFile.getPath(), targetFile.getPath());
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", hadoopDir);

        File parentDir = new File(allDirFull);

        FileSplit filesInDir = new FileSplit(parentDir, allowedExtensions, randNumGen);

        ParentPathLabelGenerator labelMaker = new ParentPathLabelGenerator();

        BalancedPathFilter pathFilter = new BalancedPathFilter(randNumGen, allowedExtensions, labelMaker);

        InputSplit[] filesInDirSplit = filesInDir.sample(pathFilter, 80, 20);
        InputSplit trainData = filesInDirSplit[0];
        InputSplit valData = filesInDirSplit[1];

        System.out.println("directory all: " + allDirFull);
        System.out.println("directory train: " + trainDirFull);
        System.out.println("directory val: " + valDirFull);
        System.out.println("number all files: " + filesInDir.length());
        System.out.println("number train files: " + trainData.length());
        System.out.println("number val files: " + valData.length());

        /* Delete train and validation dir */
        deleteFolder(trainDirFull);
        deleteFolder(valDirFull);

        /* crete train and validation data */
        createFileSet(trainData, trainDirFull);
        createFileSet(valData, valDirFull);

        System.out.println("Finish");
    }
}
