package newSort;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import newSort.NewSort3.SortBlock;

import quickSort.QuickSort;
import tools.MyArrayUtil;
import tools.TestTools;

public class NewSort4 <E extends Comparable> extends QuickSort<E>{
	ExecutorService executor;
	int threadsNum,arrayLength,pos,first;
	List<Callable<Object>> workers;
	List<SortBlock> works;
	//SortBlock[] works;
	BlockingQueue<SortBlock> quickSortTasks;
	boolean endArray = false;
	boolean firstSet = false;
	TestTools tt;


	public NewSort4() {
		threadsNum = Runtime.getRuntime().availableProcessors();
		tt = new TestTools();
	}


	@SuppressWarnings("unchecked")
	public void sort(E[] array){
		this.array = array;
		arrayLength = array.length;
		executor = Executors.newFixedThreadPool(threadsNum);
		workers = new ArrayList<Callable<Object>>(threadsNum);
		works = new ArrayList<SortBlock>();
		//works = new SortBlock[arrayLength];
		//quickSortTasks = new LinkedBlockingQueue<SortBlock>();
		pos = 0;

		try {

			while(!endArray){
				if(array[pos].compareTo(array[pos+1]) <= 0)
					ascendingOrder();
				else
					descendindOrder();

				pos++;  //次の数を調べる

				if(pos+1 > arrayLength-1){
					works.add(new SortBlock(pos,pos));
					break;
				}
			}


			executor.invokeAll(workers);

			//マージするメソッド
			parallelMergeSort();

			executor.invokeAll(workers);
		}catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
		executor.shutdown();

	}

	//昇順メソッド
	@SuppressWarnings("unchecked")
	public void ascendingOrder(){
		first = pos;
		while(array[pos].compareTo(array[pos+1]) <= 0){
			if(pos+1 >= arrayLength-1){
				works.add(new SortBlock(first,pos+1));
				endArray = true;
				return;
			}
			pos++;
		}
		works.add(new SortBlock(first,pos));
		return;
	}

	//降順メソッド
	@SuppressWarnings("unchecked")
	public void descendindOrder(){
		first = pos;

		while(array[pos].compareTo(array[pos+1]) > 0){
			if(pos+1 >= arrayLength-1){
				workers.add(Executors.callable(new QuickSortWorker(first,pos+1)));
				works.add(new SortBlock(first,pos+1));
				endArray = true;
				return;
			}
			pos++;
		}

		if((pos - first) < 10){   //更新
			quickSort(first,pos);
		}else
			workers.add(Executors.callable(new QuickSortWorker(first,pos)));//修正、もっと効率よくしたい

		works.add(new SortBlock(first,pos));
		return;
	}

	//マージする部分
	public void parallelMergeSort(){
		int workSize = 0,tempSize = 0;
		int worksPointer = 0;
		SortBlock block1,block2;
		//System.out.println("workSize : "+works.size());
		while(true){
			workSize = works.size() - tempSize;  //使ったworkはもう見る必要はないためカットする
			tempSize = workSize + tempSize;   //使ったworkのサイズを保存

			while(workSize > 1){
				//二つのworksを取り出し、left,rightを取り出す
				block1 = works.get(worksPointer++);
				block2 = works.get(worksPointer++);


				if((block2.right - block1.left) < 10){
					merge(block1.left,block1.right,block2.right,new Object[block2.right - block1.left + 1]);
				}else
					workers.add(Executors.callable(new MergeSortWorker(block1.left,block1.right,block2.right)));

				works.add(new SortBlock(block1.left,block2.right));

				workSize = workSize-2;
				//一つ余りが出たら後ろに回す
				if(workSize == 1){
					SortBlock tmp = works.get(worksPointer++);
					works.add(tmp);
					break;
				}
			}


			try {
				executor.invokeAll(workers);
			} catch (InterruptedException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}

			if(works.size() - tempSize == 2){  //範囲の情報が二つのときは最後にマージして終了
				block1 = works.get(worksPointer++);
				block2 = works.get(worksPointer);
				merge(block1.left,block1.right,block2.right,new Object[arrayLength]);
				break;
			}else if(works.size() - tempSize == 1)  //範囲の情報が一つのときは完成しているので終了
				break;
		}
	}




	@SuppressWarnings("unchecked")
	public synchronized void merge(int left,int mid,int right,Object[] buff){
		int i = left,j = mid + 1,k = 0;
		//Object[] buff = buff2;

		while(i <= mid && j <= right) {
			if(array[i].compareTo(array[j]) < 0)
				buff[k++] = array[i++];
			else
				buff[k++] = array[j++];
		}

		while(i <= mid)
			buff[k++] = array[i++];
		while(j <= right)
			buff[k++] = array[j++];
		for(i = left;i <= right; i++)
			array[i] = (E) buff[i - left];
	}



	class MergeSortWorker implements Runnable{
		int left,right,mid;
		Object[] buff;
		public MergeSortWorker(int left,int right){
			this.left = left;
			this.right = right;
			mid = (left + right) / 2;
			buff = new Object[right - left + 1];
		}

		public MergeSortWorker(int left,int mid,int right){
			this.left = left;
			this.right = right;
			this.mid = mid;
			buff = new Object[right - left + 1];
		}
		public void run(){
			merge(left,mid,right,buff);
		}
	}


	class SortBlock{
		private int left,right;
		SortBlock(int left,int right){
			this.left = left;
			this.right = right;
		}

	}

	class QuickSortWorker implements Runnable {
		int left,right;
		public QuickSortWorker(int left,int right){
			this.left = left;
			this.right = right;
		}

		public void run() {
			quickSort(left,right);
		}
	}
}
